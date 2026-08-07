// Page de lien de partage : /d/<hash>#<clé>. Récupère le ciphertext du
// cluster, le déchiffre DANS le navigateur (WebCrypto), et propose le
// fichier. La clé (fragment) n'atteint jamais le serveur.

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
  useTitle("Téléchargement");
  const { hash = "" } = useParams();
  const [phase, setPhase] = useState<Phase>({ kind: "ready", size: null });
  const keyB64 = location.hash.slice(1);
  const localName = keyring()[hash]?.name;

  useEffect(() => {
    if (!keyB64) {
      setPhase({ kind: "no-key" });
      return;
    }
    // Taille du ciphertext (approche la taille réelle à ~0,002 % près).
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
        throw new Error(`téléchargement refusé (${resp.status})`);
      }
      const plain = decryptStream(resp.body, key, (done) =>
        setPhase({ kind: "downloading", done }),
      );
      const blob = await new Response(plain).blob();
      const name = localName || `${hash.slice(0, 12)}.bin`;
      keyringImport(hash, keyB64);
      const url = URL.createObjectURL(blob);
      setPhase({ kind: "done", size: blob.size, url, name });
      // Déclenche la sauvegarde immédiatement.
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
          <h1 className="font-semibold">{localName ?? "Fichier chiffré"}</h1>
          <p className="text-xs text-muted-foreground font-mono mt-1">{hash.slice(0, 24)}…</p>
        </div>

        {phase.kind === "no-key" && (
          <p className="text-sm text-destructive flex items-center justify-center gap-2">
            <TriangleAlert size={14} /> Lien sans clé (#…) — indéchiffrable.
          </p>
        )}

        {phase.kind === "ready" && (
          <>
            {phase.size !== null && (
              <p className="text-sm text-muted-foreground">{formatSize(phase.size)} (chiffré)</p>
            )}
            <button
              onClick={() => void start()}
              className="w-full flex items-center justify-center gap-2 text-sm bg-accent hover:bg-border-bright border border-border-bright rounded-lg px-4 py-2.5 transition-colors"
            >
              <DownloadIcon size={15} /> Télécharger et déchiffrer
            </button>
            <p className="text-xs text-muted-foreground flex items-center justify-center gap-1.5">
              <ShieldCheck size={12} /> Déchiffré dans ce navigateur — le serveur ne voit rien.
            </p>
          </>
        )}

        {phase.kind === "downloading" && (
          <p className="text-sm text-muted-foreground flex items-center justify-center gap-2">
            <Loader2 size={14} className="animate-spin" /> {formatSize(phase.done)} déchiffrés…
          </p>
        )}

        {phase.kind === "done" && (
          <div className="space-y-2">
            <p className="text-sm text-success flex items-center justify-center gap-2">
              <ShieldCheck size={14} /> {formatSize(phase.size)} — intégrité vérifiée.
            </p>
            <a href={phase.url} download={phase.name} className="text-xs text-primary underline">
              Ré-enregistrer {phase.name}
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
