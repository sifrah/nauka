// Decrypting streaming Service Worker.
//
// Intercepts /stream/<hash> and serves the PLAINTEXT to <video>/<audio>
// without ever loading the whole file: for each requested range, only the
// encrypted chunks (1 MiB) covering it are pulled from the cluster (Range
// requests on the ciphertext), decrypted, and returned.
//
// Two pitfalls fixed here, both hit for real:
//  1. A Service Worker's in-memory state is volatile — the browser stops and
//     restarts it between two events. So the keys live in IndexedDB, not in
//     a Map.
//  2. A Service Worker that streams a response for tens of seconds gets
//     killed (the player then receives a 503). Each response is therefore
//     capped at MAX_RESPONSE and returned as a 206: the player chains Range
//     requests, exactly as it would with a classic media server.

const CHUNK_SIZE = 1024 * 1024;
const TAG_SIZE = 16;
const HEADER_SIZE = 12; // "YGE1" + prefix(8)
const FRAME_OVERHEAD = 5; // length u32 LE + flags u8
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

/** In-memory cache of imported keys (rebuilt after a restart). */
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

/** Offset of the chunk at index i within the ciphertext, and its frame size. */
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

/** Reads a byte range of the CIPHERTEXT from the cluster. */
async function fetchCipherRange(hash, from, to) {
  const resp = await fetch(`/f/${hash}`, { headers: { Range: `bytes=${from}-${to}` } });
  if (!resp.ok) throw new Error(`range ${from}-${to}: HTTP ${resp.status}`);
  return new Uint8Array(await resp.arrayBuffer());
}

async function ensurePrefix(hash, entry) {
  if (entry.prefix) return entry.prefix;
  const header = await fetchCipherRange(hash, 0, HEADER_SIZE - 1);
  if (header[0] !== 0x59 || header[1] !== 0x47 || header[2] !== 0x45 || header[3] !== 0x31) {
    throw new Error("not a yogfile encrypted stream");
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
    return new Response(`unreadable keyring: ${err}`, { status: 500 });
  }
  if (!entry) {
    return new Response("key missing — open the full share link (#key)", { status: 404 });
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

    // A full body (rather than a ReadableStream): the response is short, and
    // the media engine consumes it with no risk of the worker being stopped
    // midway. The chunks are decrypted in parallel.
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
    return new Response(`decryption error: ${err}`, { status: 500 });
  }
}
