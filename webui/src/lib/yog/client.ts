// Client de l'API yogfile + trousseau local.
//
// Zéro-connaissance oblige : le serveur ne connaît que des hashes de
// ciphertext. Les CLÉS des fichiers uploadés depuis ce navigateur sont
// conservées dans localStorage — c'est le trousseau. Un fichier dont la
// clé n'est pas dans le trousseau (uploadé ailleurs) reste listé mais
// indéchiffrable ici, sauf à coller son lien complet.

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
  decodeKey(keyB64); // valide
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
  link: string; // lien de partage complet, clé comprise
}

/** Chiffre puis uploade un fichier ; enregistre la clé dans le trousseau. */
export async function uploadEncrypted(
  file: File,
  onProgress?: (fraction: number) => void,
): Promise<UploadResult> {
  const rawKey = generateKey();
  const encrypted = encryptStream(file.stream(), rawKey, (bytes) => {
    onProgress?.(Math.min(0.99, bytes / Math.max(1, file.size)));
  });

  // fetch(duplex) streaming n'est pas encore fiable partout : on
  // matérialise le ciphertext en Blob (streaming du chiffrement, mémoire
  // le temps de l'envoi) — suffisant jusqu'à quelques Go.
  const ctBlob = await new Response(encrypted).blob();
  if (ctBlob.size !== ciphertextSize(file.size)) {
    throw new Error("taille de ciphertext inattendue (bug de chiffrement ?)");
  }

  const resp = await fetch("/api/upload", { method: "POST", body: ctBlob });
  if (!resp.ok) throw new Error(`upload refusé (${resp.status}): ${await resp.text()}`);
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

/** Lien de partage : la clé vit dans le fragment, jamais envoyée au serveur. */
export function shareLink(hash: string, keyB64: string): string {
  return `${location.origin}/d/${hash}#${keyB64}`;
}
