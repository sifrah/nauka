// Chiffrement de bout en bout, côté navigateur — WebCrypto AES-256-GCM.
// Format STRICTEMENT identique à la crate Rust `yog-crypto` :
//   en-tête  : "YGE1" ‖ préfixe_nonce(8)
//   par chunk: longueur_ct u32 LE ‖ flags u8 (1 = dernier) ‖ ct(+tag 16 o)
//   nonce    : préfixe(8) ‖ compteur u32 BE ; AAD = [flags]
// Un fichier chiffré ici se déchiffre avec `yog-node download`, et
// réciproquement.

export const CHUNK_SIZE = 1024 * 1024;
const TAG_SIZE = 16;
const MAGIC = new Uint8Array([0x59, 0x47, 0x45, 0x31]); // "YGE1"
const FLAG_LAST = 1;

const B64URL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

export function encodeKey(key: Uint8Array): string {
  let out = "";
  for (let i = 0; i < key.length; i += 3) {
    const [a, b, c] = [key[i], key[i + 1], key[i + 2]];
    out += B64URL[a >> 2];
    out += B64URL[((a & 3) << 4) | (b === undefined ? 0 : b >> 4)];
    if (b !== undefined) out += B64URL[((b & 15) << 2) | (c === undefined ? 0 : c >> 6)];
    if (c !== undefined) out += B64URL[c & 63];
  }
  return out;
}

export function decodeKey(s: string): Uint8Array {
  const cleaned = s.trim();
  const bytes: number[] = [];
  let buffer = 0;
  let bits = 0;
  for (const ch of cleaned) {
    const v = B64URL.indexOf(ch);
    if (v < 0) throw new Error("clé invalide");
    buffer = (buffer << 6) | v;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      bytes.push((buffer >> bits) & 0xff);
    }
  }
  const key = new Uint8Array(bytes);
  if (key.length !== 32) throw new Error("clé invalide (32 octets attendus)");
  return key;
}

export function generateKey(): Uint8Array {
  const key = new Uint8Array(32);
  crypto.getRandomValues(key);
  return key;
}

async function importKey(raw: Uint8Array): Promise<CryptoKey> {
  return crypto.subtle.importKey("raw", raw as BufferSource, "AES-GCM", false, [
    "encrypt",
    "decrypt",
  ]);
}

function nonceFor(prefix: Uint8Array, counter: number): Uint8Array {
  const nonce = new Uint8Array(12);
  nonce.set(prefix);
  new DataView(nonce.buffer).setUint32(8, counter, false); // big-endian
  return nonce;
}

/** Taille du ciphertext pour une taille de plaintext donnée. */
export function ciphertextSize(plainSize: number): number {
  const chunks = Math.max(1, Math.ceil(plainSize / CHUNK_SIZE));
  return 12 + plainSize + chunks * (4 + 1 + TAG_SIZE);
}

/**
 * Chiffre un flux (fichier) en flux : mémoire bornée à ~un chunk.
 * `onProgress(octets_de_plaintext_traités)` optionnel.
 */
export function encryptStream(
  input: ReadableStream<Uint8Array>,
  rawKey: Uint8Array,
  onProgress?: (bytes: number) => void,
): ReadableStream<Uint8Array> {
  const prefix = new Uint8Array(8);
  crypto.getRandomValues(prefix);

  let keyPromise: Promise<CryptoKey> | null = null;
  const reader = input.getReader();
  let pending = new Uint8Array(0);
  let counter = 0;
  let done = false;
  let sentHeader = false;
  let processed = 0;

  async function nextChunk(): Promise<Uint8Array | null> {
    // Accumule jusqu'à CHUNK_SIZE + 1 octet de lookahead (pour connaître
    // le dernier chunk sans lecture supplémentaire).
    while (!done && pending.length <= CHUNK_SIZE) {
      const { value, done: d } = await reader.read();
      if (d) {
        done = true;
        break;
      }
      const merged = new Uint8Array(pending.length + value.length);
      merged.set(pending);
      merged.set(value, pending.length);
      pending = merged;
    }
    if (counter > 0 && pending.length === 0 && done) return null;
    const take = Math.min(CHUNK_SIZE, pending.length);
    const chunk = pending.slice(0, take);
    pending = pending.slice(take);
    return chunk;
  }

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      keyPromise ??= importKey(rawKey);
      const key = await keyPromise;
      if (!sentHeader) {
        const header = new Uint8Array(12);
        header.set(MAGIC);
        header.set(prefix, 4);
        controller.enqueue(header);
        sentHeader = true;
        return;
      }
      const chunk = await nextChunk();
      if (chunk === null) {
        controller.close();
        return;
      }
      const last = done && pending.length === 0;
      const flags = last ? FLAG_LAST : 0;
      const ct = new Uint8Array(
        await crypto.subtle.encrypt(
          {
            name: "AES-GCM",
            iv: nonceFor(prefix, counter) as BufferSource,
            additionalData: new Uint8Array([flags]) as BufferSource,
          },
          key,
          chunk as BufferSource,
        ),
      );
      const frame = new Uint8Array(5 + ct.length);
      new DataView(frame.buffer).setUint32(0, ct.length, true); // little-endian
      frame[4] = flags;
      frame.set(ct, 5);
      counter += 1;
      processed += chunk.length;
      onProgress?.(processed);
      controller.enqueue(frame);
      if (last) controller.close();
    },
    cancel(reason) {
      void reader.cancel(reason);
    },
  });
}

/** Déchiffre un flux chiffré au format YGE1. Rejette si altéré/tronqué. */
export function decryptStream(
  input: ReadableStream<Uint8Array>,
  rawKey: Uint8Array,
  onProgress?: (bytes: number) => void,
): ReadableStream<Uint8Array> {
  const reader = input.getReader();
  let keyPromise: Promise<CryptoKey> | null = null;
  let pending = new Uint8Array(0);
  let inputDone = false;
  let headerParsed = false;
  let prefix = new Uint8Array(8);
  let counter = 0;
  let sawLast = false;
  let processed = 0;

  async function fill(target: number): Promise<boolean> {
    while (pending.length < target && !inputDone) {
      const { value, done } = await reader.read();
      if (done) {
        inputDone = true;
        break;
      }
      const merged = new Uint8Array(pending.length + value.length);
      merged.set(pending);
      merged.set(value, pending.length);
      pending = merged;
    }
    return pending.length >= target;
  }

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      keyPromise ??= importKey(rawKey);
      const key = await keyPromise;
      if (!headerParsed) {
        if (!(await fill(12))) throw new Error("flux tronqué (en-tête)");
        for (let i = 0; i < 4; i++) {
          if (pending[i] !== MAGIC[i]) throw new Error("pas un flux chiffré yogfile");
        }
        prefix = pending.slice(4, 12);
        pending = pending.slice(12);
        headerParsed = true;
      }
      if (sawLast) {
        if (pending.length > 0 || (await fill(1))) {
          throw new Error("données après le dernier chunk");
        }
        controller.close();
        return;
      }
      if (!(await fill(5))) throw new Error("flux tronqué (chunk manquant)");
      const len = new DataView(pending.buffer, pending.byteOffset).getUint32(0, true);
      const flags = pending[4];
      if (len < TAG_SIZE || len > CHUNK_SIZE + TAG_SIZE) {
        throw new Error("taille de chunk invalide");
      }
      if (!(await fill(5 + len))) throw new Error("chunk incomplet");
      const ct = pending.slice(5, 5 + len);
      pending = pending.slice(5 + len);
      let plain: ArrayBuffer;
      try {
        plain = await crypto.subtle.decrypt(
          {
            name: "AES-GCM",
            iv: nonceFor(prefix, counter) as BufferSource,
            additionalData: new Uint8Array([flags]) as BufferSource,
          },
          key,
          ct as BufferSource,
        );
      } catch {
        throw new Error("déchiffrement refusé : données altérées ou mauvaise clé");
      }
      counter += 1;
      processed += plain.byteLength;
      onProgress?.(processed);
      if (flags & FLAG_LAST) sawLast = true;
      controller.enqueue(new Uint8Array(plain));
    },
    cancel(reason) {
      void reader.cancel(reason);
    },
  });
}
