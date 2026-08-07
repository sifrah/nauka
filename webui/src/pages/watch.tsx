// Lecteur média chiffré : /w/<hash>#<clé>.
//
// Le fichier est déchiffré dans le navigateur (WebCrypto), puis donné à
// <video> sous forme de Blob URL : la lecture et le SEEK sont alors
// natifs et instantanés, sans qu'un seul octet en clair n'ait transité.
// Le serveur n'a servi que du ciphertext.

import { useEffect, useRef, useState } from "react";
import { useParams } from "react-router";
import { Loader2, Lock, ShieldCheck, TriangleAlert } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { formatSize } from "../lib/format";
import { formatError } from "../lib/errors";
import { decodeKey, decryptStream } from "../lib/yog/crypto";
import { keyring, keyringImport } from "../lib/yog/client";
import { guessMime, plainSizeFromCipher } from "../lib/yog/stream";

/** Au-delà, la lecture en ligne coûterait trop de mémoire au navigateur. */
const MAX_INLINE = 600 * 1024 * 1024;

type Phase =
  | { kind: "loading"; done: number; total: number }
  | { kind: "ready"; url: string; size: number }
  | { kind: "too-big"; size: number }
  | { kind: "error"; message: string };

export function WatchPage() {
  const { hash = "" } = useParams();
  const entry = keyring()[hash];
  const name = entry?.name ?? null;
  useTitle(name ?? "Lecture");
  const [phase, setPhase] = useState<Phase>({ kind: "loading", done: 0, total: 0 });
  const urlRef = useRef<string | null>(null);
  const keyB64 = location.hash.slice(1) || entry?.key || "";

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (!keyB64) throw new Error("lien sans clé (#…) — lecture impossible");
        const key = decodeKey(keyB64);
        const head = await fetch(`/f/${hash}`, { method: "HEAD" });
        if (!head.ok) throw new Error(`fichier introuvable (${head.status})`);
        const cipherSize = Number(head.headers.get("content-length") ?? 0);
        const plainSize = plainSizeFromCipher(cipherSize);
        if (plainSize > MAX_INLINE) {
          setPhase({ kind: "too-big", size: plainSize });
          return;
        }
        setPhase({ kind: "loading", done: 0, total: plainSize });

        const resp = await fetch(`/f/${hash}`);
        if (!resp.ok || !resp.body) throw new Error(`téléchargement refusé (${resp.status})`);
        const plain = decryptStream(resp.body, key, (done) => {
          if (!cancelled) setPhase({ kind: "loading", done, total: plainSize });
        });
        const blob = await new Response(plain).blob();
        if (cancelled) return;
        const typed = new Blob([blob], { type: guessMime(name) });
        const url = URL.createObjectURL(typed);
        urlRef.current = url;
        keyringImport(hash, keyB64);
        setPhase({ kind: "ready", url, size: typed.size });
      } catch (e) {
        if (!cancelled) setPhase({ kind: "error", message: formatError(e) });
      }
    })();
    return () => {
      cancelled = true;
      if (urlRef.current) URL.revokeObjectURL(urlRef.current);
    };
  }, [hash, keyB64, name]);

  return (
    <div className="p-6 h-full flex flex-col items-center justify-center gap-4">
      <div className="w-full max-w-4xl">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Lock size={14} className="text-muted-foreground" />
            <h1 className="text-sm font-medium">{name ?? "Média chiffré"}</h1>
          </div>
          {phase.kind === "ready" && (
            <span className="text-xs text-muted-foreground font-mono">
              {formatSize(phase.size)}
            </span>
          )}
        </div>

        <div className="card-surface rounded-lg overflow-hidden aspect-video flex items-center justify-center bg-black">
          {phase.kind === "error" && (
            <p className="text-sm text-destructive flex items-center gap-2 p-6 text-center">
              <TriangleAlert size={14} /> {phase.message}
            </p>
          )}
          {phase.kind === "too-big" && (
            <div className="text-center p-6 space-y-2">
              <p className="text-sm text-muted-foreground">
                {formatSize(phase.size)} — trop volumineux pour la lecture en ligne.
              </p>
              <a href={`/d/${hash}#${keyB64}`} className="text-xs text-primary underline">
                Télécharger et déchiffrer
              </a>
            </div>
          )}
          {phase.kind === "loading" && (
            <div className="w-2/3 space-y-3 text-center">
              <span className="flex items-center justify-center gap-2 text-sm text-muted">
                <Loader2 size={16} className="animate-spin" /> Déchiffrement…
              </span>
              <div className="h-1.5 bg-accent rounded-full overflow-hidden">
                <div
                  className="h-full bg-primary transition-all"
                  style={{
                    width: `${phase.total ? Math.min(100, (phase.done / phase.total) * 100) : 0}%`,
                  }}
                />
              </div>
              <span className="text-xs text-muted-foreground font-mono">
                {formatSize(phase.done)}
                {phase.total ? ` / ${formatSize(phase.total)}` : ""}
              </span>
            </div>
          )}
          {phase.kind === "ready" && (
            <video src={phase.url} controls autoPlay className="w-full h-full" />
          )}
        </div>

        <p className="text-xs text-muted-foreground flex items-center justify-center gap-1.5 mt-3">
          <ShieldCheck size={12} /> Déchiffré dans ce navigateur — le serveur n'a servi que du
          ciphertext. Lecture et déplacement dans la vidéo entièrement locaux.
        </p>
      </div>
    </div>
  );
}
