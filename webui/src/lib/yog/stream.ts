// Helpers de lecture média.
//
// Le lecteur déchiffre le fichier dans le navigateur puis le donne à
// <video> via un Blob URL : le seek est alors natif et instantané, et le
// serveur n'a vu passer que du ciphertext.
//
// Pourquoi pas un Service Worker qui déchiffrerait à la volée (ce qui
// éviterait de tout charger en mémoire) : le moteur média de Chrome
// n'accepte pas les réponses produites par un Service Worker pour un
// élément <video> — la requête reste bloquée à zéro octet, alors que le
// même flux se lit parfaitement via fetch(). Le serveur supporte les
// requêtes Range (cf. GET /f/{hash}), donc la lecture par plages reste
// possible le jour où MediaSource Extensions sera branché (fMP4).

import { CHUNK_SIZE } from "./crypto";

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
