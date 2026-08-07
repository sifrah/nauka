// End-to-end encryption, in the browser — WebCrypto AES-256-GCM.
// Format STRICTLY identical to the Rust crate `nauka-crypto`:
//   header   : "YGE1" ‖ nonce_prefix(8)
//   per chunk: ct_length u32 LE ‖ flags u8 (1 = last) ‖ ct(+16-byte tag)
//   nonce    : prefix(8) ‖ counter u32 BE ; AAD = [flags]
// A file encrypted here can be decrypted with `nauka-node download`, and
// the other way around.

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
    if (v < 0) throw new Error("invalid key");
    buffer = (buffer << 6) | v;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      bytes.push((buffer >> bits) & 0xff);
    }
  }
  const key = new Uint8Array(bytes);
  if (key.length !== 32) throw new Error("invalid key (32 bytes expected)");
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

/** Ciphertext size for a given plaintext size. */
export function ciphertextSize(plainSize: number): number {
  const chunks = Math.max(1, Math.ceil(plainSize / CHUNK_SIZE));
  return 12 + plainSize + chunks * (4 + 1 + TAG_SIZE);
}

/**
 * Encrypts a stream (a file) as a stream: memory stays bounded to ~one chunk.
 * `onProgress(plaintext_bytes_processed)` is optional.
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
    // Accumulate up to CHUNK_SIZE + 1 byte of lookahead (so the last chunk
    // can be identified without an extra read).
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

/** Decrypts a YGE1-encrypted stream. Rejects tampered/truncated input. */
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
        if (!(await fill(12))) throw new Error("truncated stream (header)");
        for (let i = 0; i < 4; i++) {
          if (pending[i] !== MAGIC[i]) throw new Error("not a yogfile encrypted stream");
        }
        prefix = pending.slice(4, 12);
        pending = pending.slice(12);
        headerParsed = true;
      }
      if (sawLast) {
        if (pending.length > 0 || (await fill(1))) {
          throw new Error("data after the last chunk");
        }
        controller.close();
        return;
      }
      if (!(await fill(5))) throw new Error("truncated stream (missing chunk)");
      const len = new DataView(pending.buffer, pending.byteOffset).getUint32(0, true);
      const flags = pending[4];
      if (len < TAG_SIZE || len > CHUNK_SIZE + TAG_SIZE) {
        throw new Error("invalid chunk size");
      }
      if (!(await fill(5 + len))) throw new Error("incomplete chunk");
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
        throw new Error("decryption refused: tampered data or wrong key");
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
