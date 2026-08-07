// Encrypted media playback.
//
// Nominal mode: a Service Worker serves /stream/<hash> in the clear from the
// cluster's ciphertext, range by range — nothing is loaded ahead of time, and
// a seek costs a single round trip. The key reaches the worker through
// IndexedDB (never over the network: it comes from the URL fragment).
//
// IndexedDB rather than postMessage: the browser stops and restarts a Service
// Worker at will, so any in-memory state is gone between two events.
//
// Fallback: if the worker is unavailable (insecure context, restrictive
// browser), the player decrypts the whole file in memory and goes through a
// Blob URL.

import { CHUNK_SIZE } from "./crypto";

const DB_NAME = "yogfile-streams";
const STORE = "keys";

const TAG_SIZE = 16;
const HEADER_SIZE = 12;
const FRAME_OVERHEAD = 5;

/** Plaintext size derived from the ciphertext size (YGE1 format). */
export function plainSizeFromCipher(cipherSize: number): number {
  const perChunk = FRAME_OVERHEAD + TAG_SIZE;
  const body = cipherSize - HEADER_SIZE;
  let chunks = Math.max(1, Math.ceil(body / (CHUNK_SIZE + perChunk)));
  for (let i = 0; i < 4; i++) {
    const plain = body - chunks * perChunk;
    const expected = Math.max(1, Math.ceil(plain / CHUNK_SIZE));
    if (expected === chunks) return plain;
    chunks = expected;
  }
  return body - chunks * perChunk;
}

export function isStreamable(name: string | null | undefined, mime?: string): boolean {
  if (mime?.startsWith("video/") || mime?.startsWith("audio/")) return true;
  const ext = (name ?? "").toLowerCase().split(".").pop() ?? "";
  return ["mp4", "webm", "m4v", "mov", "ogg", "ogv", "mp3", "m4a", "opus", "wav", "flac"].includes(
    ext,
  );
}

export function guessMime(name: string | null | undefined): string {
  const ext = (name ?? "").toLowerCase().split(".").pop() ?? "";
  const map: Record<string, string> = {
    mp4: "video/mp4",
    m4v: "video/mp4",
    mov: "video/mp4",
    webm: "video/webm",
    ogv: "video/ogg",
    ogg: "audio/ogg",
    opus: "audio/ogg",
    mp3: "audio/mpeg",
    m4a: "audio/mp4",
    wav: "audio/wav",
    flac: "audio/flac",
  };
  return map[ext] ?? "video/mp4";
}

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => {
      if (!req.result.objectStoreNames.contains(STORE)) req.result.createObjectStore(STORE);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

/**
 * Prepares range-based playback: hands the key over to the worker and waits
 * until it CONTROLS the page (with no controller, the <video> request would
 * go to the server, which only holds ciphertext).
 * Returns the URL to give to <video>, or null if streaming is unavailable
 * (the caller then falls back to full decryption).
 */
export async function prepareStream(
  hash: string,
  rawKey: Uint8Array,
  plainSize: number,
  name: string | null,
): Promise<string | null> {
  if (!("serviceWorker" in navigator)) return null;
  try {
    const keyCopy = new Uint8Array(rawKey);
    const db = await openDb();
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(STORE, "readwrite");
      tx.objectStore(STORE).put(
        { rawKey: keyCopy.buffer as ArrayBuffer, plainSize, mime: guessMime(name) },
        hash,
      );
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });

    await navigator.serviceWorker.register("/sw-stream.js", { scope: "/" });
    await navigator.serviceWorker.ready;
    if (!navigator.serviceWorker.controller) {
      await new Promise<void>((resolve) => {
        navigator.serviceWorker.addEventListener("controllerchange", () => resolve(), {
          once: true,
        });
        setTimeout(resolve, 2000);
      });
    }
    if (!navigator.serviceWorker.controller) return null;

    // Check that the worker really serves decrypted bytes before pointing
    // <video> at it.
    const probe = await fetch(`/stream/${hash}`, { headers: { Range: "bytes=0-11" } });
    if (probe.status !== 206) return null;
    const magic = new Uint8Array(await probe.arrayBuffer());
    // A still-encrypted stream would start with "YGE1": that would mean the
    // request went to the server instead of the worker.
    const isCipher = magic[0] === 0x59 && magic[1] === 0x47 && magic[2] === 0x45;
    return isCipher ? null : `/stream/${hash}`;
  } catch {
    return null;
  }
}
