// Service Worker de streaming déchiffré.
//
// Intercepte /stream/<hash> et sert le PLAINTEXT à <video>/<audio> sans
// jamais charger le fichier entier : pour chaque plage demandée, seuls les
// chunks chiffrés (1 Mio) qui la couvrent sont tirés du cluster (requêtes
// Range sur le ciphertext), déchiffrés, et rendus.
//
// Deux pièges corrigés ici, tous deux rencontrés en vrai :
//  1. L'état mémoire d'un Service Worker est volatile — le navigateur
//     l'arrête et le redémarre entre deux événements. Les clés vivent donc
//     dans IndexedDB, pas dans une Map.
//  2. Un Service Worker qui streame une réponse pendant des dizaines de
//     secondes est tué (le lecteur reçoit alors un 503). Chaque réponse est
//     donc bornée à MAX_RESPONSE et renvoyée en 206 : le lecteur enchaîne
//     les requêtes Range, exactement comme avec un serveur média classique.

const CHUNK_SIZE = 1024 * 1024;
const TAG_SIZE = 16;
const HEADER_SIZE = 12; // "YGE1" + préfixe(8)
const FRAME_OVERHEAD = 5; // longueur u32 LE + flags u8
const MAX_RESPONSE = 4 * CHUNK_SIZE;

const DB_NAME = "yogfile-streams";
const STORE = "keys";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => {
      if (!req.result.objectStoreNames.contains(STORE)) req.result.createObjectStore(STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function loadEntry(hash) {
  const db = await openDb();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readonly").objectStore(STORE).get(hash);
    tx.onsuccess = () => resolve(tx.result ?? null);
    tx.onerror = () => reject(tx.error);
  });
}

/** Cache mémoire des clés importées (reconstruit après un redémarrage). */
const imported = new Map();

async function entryFor(hash) {
  if (imported.has(hash)) return imported.get(hash);
  const stored = await loadEntry(hash);
  if (!stored) return null;
  const key = await crypto.subtle.importKey("raw", stored.rawKey, "AES-GCM", false, ["decrypt"]);
  const entry = { key, plainSize: stored.plainSize, mime: stored.mime, prefix: null };
  imported.set(hash, entry);
  return entry;
}

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin) return;
  const match = url.pathname.match(/^\/stream\/([a-f0-9]+)$/);
  if (!match) return;
  event.respondWith(handleStream(match[1], event.request));
});

/** Offset du chunk d'index i dans le ciphertext, et sa taille de frame. */
function frameAt(index, plainSize) {
  const fullChunks = Math.floor(plainSize / CHUNK_SIZE);
  const plainBefore = Math.min(index, fullChunks) * CHUNK_SIZE;
  const offset = HEADER_SIZE + plainBefore + index * (FRAME_OVERHEAD + TAG_SIZE);
  const plainLen =
    index < fullChunks ? CHUNK_SIZE : Math.max(0, plainSize - fullChunks * CHUNK_SIZE);
  return { offset, frameLen: FRAME_OVERHEAD + plainLen + TAG_SIZE };
}

function nonceFor(prefix, counter) {
  const nonce = new Uint8Array(12);
  nonce.set(prefix);
  new DataView(nonce.buffer).setUint32(8, counter, false);
  return nonce;
}

/** Lit une plage d'octets du CIPHERTEXT sur le cluster. */
async function fetchCipherRange(hash, from, to) {
  const resp = await fetch(`/f/${hash}`, { headers: { Range: `bytes=${from}-${to}` } });
  if (!resp.ok) throw new Error(`range ${from}-${to}: HTTP ${resp.status}`);
  return new Uint8Array(await resp.arrayBuffer());
}

async function ensurePrefix(hash, entry) {
  if (entry.prefix) return entry.prefix;
  const header = await fetchCipherRange(hash, 0, HEADER_SIZE - 1);
  if (header[0] !== 0x59 || header[1] !== 0x47 || header[2] !== 0x45 || header[3] !== 0x31) {
    throw new Error("pas un flux chiffré yogfile");
  }
  entry.prefix = header.slice(4, 12);
  return entry.prefix;
}

async function decryptChunk(hash, entry, index) {
  const prefix = await ensurePrefix(hash, entry);
  const { offset, frameLen } = frameAt(index, entry.plainSize);
  const frame = await fetchCipherRange(hash, offset, offset + frameLen - 1);
  const len = new DataView(frame.buffer, frame.byteOffset).getUint32(0, true);
  const flags = frame[4];
  const ct = frame.slice(5, 5 + len);
  const plain = await crypto.subtle.decrypt(
    { name: "AES-GCM", iv: nonceFor(prefix, index), additionalData: new Uint8Array([flags]) },
    entry.key,
    ct,
  );
  return new Uint8Array(plain);
}

async function handleStream(hash, request) {
  let entry;
  try {
    entry = await entryFor(hash);
  } catch (err) {
    return new Response(`trousseau illisible: ${err}`, { status: 500 });
  }
  if (!entry) {
    return new Response("clé absente — ouvrir le lien de partage complet (#clé)", { status: 404 });
  }

  try {
    const size = entry.plainSize;
    let start = 0;
    let end = size - 1;
    const rangeHeader = request.headers.get("Range");
    if (rangeHeader) {
      const m = rangeHeader.match(/bytes=(\d*)-(\d*)/);
      if (m) {
        if (m[1] === "") {
          start = Math.max(0, size - Number(m[2]));
        } else {
          start = Number(m[1]);
          if (m[2] !== "") end = Math.min(Number(m[2]), size - 1);
        }
      }
    }
    if (start >= size || start > end) {
      return new Response(null, { status: 416, headers: { "Content-Range": `bytes */${size}` } });
    }
    end = Math.min(end, start + MAX_RESPONSE - 1);

    // Corps complet (et non un ReadableStream) : la réponse est courte, et
    // le moteur média la consomme sans risque que le worker soit arrêté en
    // cours de route. Les chunks sont déchiffrés en parallèle.
    const firstChunk = Math.floor(start / CHUNK_SIZE);
    const lastChunk = Math.floor(end / CHUNK_SIZE);
    const plains = await Promise.all(
      Array.from({ length: lastChunk - firstChunk + 1 }, (_, n) =>
        decryptChunk(hash, entry, firstChunk + n),
      ),
    );
    const out = new Uint8Array(end - start + 1);
    let written = 0;
    for (let n = 0; n < plains.length; n++) {
      const chunkStart = (firstChunk + n) * CHUNK_SIZE;
      const from = Math.max(0, start - chunkStart);
      const to = Math.min(plains[n].length, end - chunkStart + 1);
      if (from < to) {
        out.set(plains[n].subarray(from, to), written);
        written += to - from;
      }
    }

    return new Response(out.subarray(0, written), {
      status: 206,
      headers: {
        "Content-Type": entry.mime || "application/octet-stream",
        "Content-Length": String(written),
        "Content-Range": `bytes ${start}-${start + written - 1}/${size}`,
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
      },
    });
  } catch (err) {
    return new Response(`erreur de déchiffrement: ${err}`, { status: 500 });
  }
}
