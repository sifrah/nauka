// Lecture média chiffrée.
//
// Mode nominal : un Service Worker sert /stream/<hash> en clair à partir
// du ciphertext du cluster, plage par plage — rien n'est chargé d'avance,
// le seek ne coûte qu'un aller-retour. La clé lui est transmise par
// IndexedDB (jamais par le réseau : elle vient du fragment de l'URL).
//
// IndexedDB et non postMessage : un Service Worker est arrêté/redémarré
// librement par le navigateur, tout état mémoire disparaît entre deux
// événements.
//
// Repli : si le worker n'est pas disponible (contexte non sécurisé,
// navigateur restrictif), le lecteur déchiffre le fichier entier en
// mémoire et passe par un Blob URL.

import { CHUNK_SIZE } from "./crypto";

const DB_NAME = "yogfile-streams";
const STORE = "keys";

const TAG_SIZE = 16;
const HEADER_SIZE = 12;
const FRAME_OVERHEAD = 5;

/** Taille du plaintext déduite de celle du ciphertext (format YGE1). */
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
 * Prépare la lecture par plages : dépose la clé pour le worker et attend
 * qu'il CONTRÔLE la page (sans contrôleur, la requête de <video> partirait
 * au serveur, qui ne détient que du ciphertext).
 * Renvoie l'URL à donner à <video>, ou null si le streaming est
 * indisponible (l'appelant se rabat sur le déchiffrement complet).
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

    // Vérifie que le worker sert bien du déchiffré avant d'y envoyer <video>.
    const probe = await fetch(`/stream/${hash}`, { headers: { Range: "bytes=0-11" } });
    if (probe.status !== 206) return null;
    const magic = new Uint8Array(await probe.arrayBuffer());
    // Un flux encore chiffré commencerait par "YGE1" : ce serait le signe
    // que la requête est passée au serveur au lieu du worker.
    const isCipher = magic[0] === 0x59 && magic[1] === 0x47 && magic[2] === 0x45;
    return isCipher ? null : `/stream/${hash}`;
  } catch {
    return null;
  }
}
