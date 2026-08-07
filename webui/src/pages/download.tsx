// Share link page: /d/<hash>#<key>. Fetches the ciphertext from the cluster,
// decrypts it IN the browser (WebCrypto), and hands the file over. The key
// (URL fragment) never reaches the server.

import { useEffect, useState } from "react";
import { useParams } from "react-router";
import { Download as DownloadIcon, Loader2, Lock, ShieldCheck, TriangleAlert } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { formatSize } from "../lib/format";
import { formatError } from "../lib/errors";
import { decodeKey, decryptStream } from "../lib/yog/crypto";
import { keyring, keyringImport } from "../lib/yog/client";

type Phase =
  | { kind: "no-key" }
  | { kind: "ready"; size: number | null }
  | { kind: "downloading"; done: number }
  | { kind: "done"; size: number; url: string; name: string }
  | { kind: "error"; message: string };

export function DownloadPage() {
  useTitle("Download");
  const { hash = "" } = useParams();
  const [phase, setPhase] = useState<Phase>({ kind: "ready", size: null });
  const keyB64 = location.hash.slice(1);
  const localName = keyring()[hash]?.name;

  useEffect(() => {
    if (!keyB64) {
      setPhase({ kind: "no-key" });
      return;
    }
    // Ciphertext size (within ~0.002% of the real size).
    fetch(`/f/${hash}`, { method: "HEAD" })
      .then((r) => {
        const len = r.headers.get("content-length");
        setPhase({ kind: "ready", size: len ? Number(len) : null });
      })
      .catch(() => setPhase({ kind: "ready", size: null }));
  }, [hash, keyB64]);

  const start = async () => {
    try {
      const key = decodeKey(keyB64);
      setPhase({ kind: "downloading", done: 0 });
      const resp = await fetch(`/f/${hash}`);
      if (!resp.ok || !resp.body) {
        throw new Error(`download rejected (${resp.status})`);
      }
      const plain = decryptStream(resp.body, key, (done) =>
        setPhase({ kind: "downloading", done }),
      );
      const blob = await new Response(plain).blob();
      const name = localName || `${hash.slice(0, 12)}.bin`;
      keyringImport(hash, keyB64);
      const url = URL.createObjectURL(blob);
      setPhase({ kind: "done", size: blob.size, url, name });
      // Trigger the save right away.
      const a = document.createElement("a");
      a.href = url;
      a.download = name;
      a.click();
    } catch (e) {
      setPhase({ kind: "error", message: formatError(e) });
    }
  };

  return (
    <div className="flex items-center justify-center h-full p-6">
      <div className="card-surface rounded-lg p-8 max-w-md w-full text-center space-y-4">
        <div className="flex justify-center">
          <div className="w-12 h-12 rounded-full bg-accent flex items-center justify-center">
            <Lock size={20} className="text-muted-foreground" />
          </div>
        </div>
        <div>
          <h1 className="font-semibold">{localName ?? "Encrypted file"}</h1>
          <p className="text-xs text-muted-foreground font-mono mt-1">{hash.slice(0, 24)}…</p>
        </div>

        {phase.kind === "no-key" && (
          <p className="text-sm text-destructive flex items-center justify-center gap-2">
            <TriangleAlert size={14} /> Link without key (#…) — cannot be decrypted.
          </p>
        )}

        {phase.kind === "ready" && (
          <>
            {phase.size !== null && (
              <p className="text-sm text-muted-foreground">{formatSize(phase.size)} (encrypted)</p>
            )}
            <button
              onClick={() => void start()}
              className="w-full flex items-center justify-center gap-2 text-sm bg-accent hover:bg-border-bright border border-border-bright rounded-lg px-4 py-2.5 transition-colors"
            >
              <DownloadIcon size={15} /> Download and decrypt
            </button>
            <p className="text-xs text-muted-foreground flex items-center justify-center gap-1.5">
              <ShieldCheck size={12} /> Decrypted in this browser — the server sees nothing.
            </p>
          </>
        )}

        {phase.kind === "downloading" && (
          <p className="text-sm text-muted-foreground flex items-center justify-center gap-2">
            <Loader2 size={14} className="animate-spin" /> {formatSize(phase.done)} decrypted…
          </p>
        )}

        {phase.kind === "done" && (
          <div className="space-y-2">
            <p className="text-sm text-success flex items-center justify-center gap-2">
              <ShieldCheck size={14} /> {formatSize(phase.size)} — integrity verified.
            </p>
            <a href={phase.url} download={phase.name} className="text-xs text-primary underline">
              Save {phase.name} again
            </a>
          </div>
        )}

        {phase.kind === "error" && (
          <p className="text-sm text-destructive flex items-center justify-center gap-2">
            <TriangleAlert size={14} /> {phase.message}
          </p>
        )}
      </div>
    </div>
  );
}
