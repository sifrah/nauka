// yogfile API client + local keyring.
//
// Zero-knowledge by design: the server only ever knows ciphertext hashes. The
// KEYS of files uploaded from this browser are kept in localStorage — that is
// the keyring. A file whose key is not in the keyring (uploaded elsewhere)
// still shows up in the listing but cannot be decrypted here, unless its full
// link is pasted in.

import { ciphertextSize, decodeKey, encodeKey, encryptStream, generateKey } from "./crypto";

export interface ClusterFile {
  hash: string;
  size: number;
  name: string | null;
  link: string;
}

export interface NodeStatus {
  addr: string;
  capacity_bytes: number;
  is_leader: boolean;
  is_self: boolean;
  /** Answering the pings of the node serving this page (down after ~15 s of silence). */
  is_alive: boolean;
}

export interface ClusterStatus {
  self_addr: string;
  leader: string | null;
  nodes: NodeStatus[];
  files: number;
  total_bytes: number;
}

export interface KeyringEntry {
  key: string; // base64url
  name: string;
  size: number;
  uploadedAt: number;
}

const KEYRING = "yogfile-keyring-v1";

export function keyring(): Record<string, KeyringEntry> {
  try {
    return JSON.parse(localStorage.getItem(KEYRING) ?? "{}") as Record<string, KeyringEntry>;
  } catch {
    return {};
  }
}

export function keyringAdd(hash: string, entry: KeyringEntry) {
  const all = keyring();
  all[hash] = entry;
  localStorage.setItem(KEYRING, JSON.stringify(all));
}

export function keyringImport(hash: string, keyB64: string) {
  decodeKey(keyB64); // validates
  const all = keyring();
  all[hash] = all[hash] ?? { key: keyB64, name: "", size: 0, uploadedAt: Date.now() };
  all[hash].key = keyB64;
  localStorage.setItem(KEYRING, JSON.stringify(all));
}

export async function fetchFiles(): Promise<ClusterFile[]> {
  const resp = await fetch("/api/files");
  if (!resp.ok) throw new Error(`GET /api/files: ${resp.status}`);
  return (await resp.json()) as ClusterFile[];
}

export async function fetchStatus(): Promise<ClusterStatus> {
  const resp = await fetch("/api/status");
  if (!resp.ok) throw new Error(`GET /api/status: ${resp.status}`);
  return (await resp.json()) as ClusterStatus;
}

export interface UploadResult {
  hash: string;
  size: number;
  link: string; // full share link, key included
}

/** Encrypts then uploads a file; stores the key in the keyring. */
export async function uploadEncrypted(
  file: File,
  onProgress?: (fraction: number) => void,
): Promise<UploadResult> {
  const rawKey = generateKey();
  const encrypted = encryptStream(file.stream(), rawKey, (bytes) => {
    onProgress?.(Math.min(0.99, bytes / Math.max(1, file.size)));
  });

  // fetch(duplex) streaming is not reliable everywhere yet: the ciphertext is
  // materialised into a Blob (encryption still streams, memory is held only
  // for the duration of the upload) — good enough up to a few GB.
  const ctBlob = await new Response(encrypted).blob();
  if (ctBlob.size !== ciphertextSize(file.size)) {
    throw new Error("unexpected ciphertext size (encryption bug?)");
  }

  const resp = await fetch("/api/upload", { method: "POST", body: ctBlob });
  if (!resp.ok) throw new Error(`upload rejected (${resp.status}): ${await resp.text()}`);
  const up = (await resp.json()) as { hash: string; size: number };
  onProgress?.(1);

  keyringAdd(up.hash, {
    key: encodeKey(rawKey),
    name: file.name,
    size: file.size,
    uploadedAt: Date.now(),
  });
  return { hash: up.hash, size: up.size, link: shareLink(up.hash, encodeKey(rawKey)) };
}

/** Share link: the key lives in the fragment, never sent to the server. */
export function shareLink(hash: string, keyB64: string): string {
  return `${location.origin}/d/${hash}#${keyB64}`;
}
