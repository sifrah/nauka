// Lecteur média chiffré : /w/<hash>#<clé>.
//
// Mode nominal — STREAMING : un Service Worker déchiffre à la volée, plage
// par plage. Rien n'est chargé d'avance : la lecture démarre tout de suite
// et un seek ne coûte qu'un aller-retour, quel que soit la taille du
// fichier. Le serveur ne sert que du ciphertext.
//
// Repli — si la lecture ne démarre pas (worker indisponible, navigateur
// restrictif) : déchiffrement complet en mémoire puis Blob URL. Simple et
// robuste, mais il faut attendre le fichier entier, donc plafonné.

import { useEffect, useRef, useState } from "react";
import { useParams } from "react-router";
import { Loader2, Lock, ShieldCheck, TriangleAlert, Zap } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { formatSize } from "../lib/format";
import { formatError } from "../lib/errors";
import { decodeKey, decryptStream } from "../lib/yog/crypto";
import { keyring, keyringImport } from "../lib/yog/client";
import { guessMime, plainSizeFromCipher, prepareStream } from "../lib/yog/stream";

/** Au-delà, le repli « tout en mémoire » n'est pas raisonnable. */
const MAX_FALLBACK = 600 * 1024 * 1024;
/** Délai laissé au streaming pour produire des métadonnées lisibles. */
const STREAM_PROBE_MS = 6000;

type Mode = "stream" | "fallback";
type Phase =
  | { kind: "init" }
  | { kind: "playing"; url: string; mode: Mode }
  | { kind: "decrypting"; done: number; total: number }
  | { kind: "too-big"; size: number }
  | { kind: "error"; message: string };

export function WatchPage() {
  const { hash = "" } = useParams();
  const entry = keyring()[hash];
  const name = entry?.name ?? null;
  useTitle(name ?? "Lecture");
  const [phase, setPhase] = useState<Phase>({ kind: "init" });
  const [size, setSize] = useState<number | null>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const blobUrlRef = useRef<string | null>(null);
  const keyB64 = location.hash.slice(1) || entry?.key || "";

  useEffect(() => {
    let cancelled = false;

    /** Repli : déchiffre tout, puis Blob URL. */
    const fallback = async (key: Uint8Array, plainSize: number) => {
      if (plainSize > MAX_FALLBACK) {
        setPhase({ kind: "too-big", size: plainSize });
        return;
      }
      setPhase({ kind: "decrypting", done: 0, total: plainSize });
      const resp = await fetch(`/f/${hash}`);
      if (!resp.ok || !resp.body) throw new Error(`téléchargement refusé (${resp.status})`);
      const plain = decryptStream(resp.body, key, (done) => {
        if (!cancelled) setPhase({ kind: "decrypting", done, total: plainSize });
      });
      const blob = new Blob([await new Response(plain).blob()], { type: guessMime(name) });
      if (cancelled) return;
      const url = URL.createObjectURL(blob);
      blobUrlRef.current = url;
      setPhase({ kind: "playing", url, mode: "fallback" });
    };

    (async () => {
      try {
        if (!keyB64) throw new Error("lien sans clé (#…) — lecture impossible");
        const key = decodeKey(keyB64);
        const head = await fetch(`/f/${hash}`, { method: "HEAD" });
        if (!head.ok) throw new Error(`fichier introuvable (${head.status})`);
        const plainSize = plainSizeFromCipher(Number(head.headers.get("content-length") ?? 0));
        if (cancelled) return;
        setSize(plainSize);
        keyringImport(hash, keyB64);

        const streamUrl = await prepareStream(hash, key, plainSize, name);
        if (cancelled) return;
        if (!streamUrl) {
          await fallback(key, plainSize);
          return;
        }
        setPhase({ kind: "playing", url: streamUrl, mode: "stream" });

        // Le streaming a-t-il vraiment démarré ? Sinon, repli silencieux.
        setTimeout(() => {
          const v = videoRef.current;
          if (cancelled || !v || v.readyState > 0) return;
          void fallback(key, plainSize).catch((e) => {
            if (!cancelled) setPhase({ kind: "error", message: formatError(e) });
          });
        }, STREAM_PROBE_MS);
      } catch (e) {
        if (!cancelled) setPhase({ kind: "error", message: formatError(e) });
      }
    })();

    return () => {
      cancelled = true;
      if (blobUrlRef.current) URL.revokeObjectURL(blobUrlRef.current);
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
          <div className="flex items-center gap-3">
            {phase.kind === "playing" && phase.mode === "stream" && (
              <span className="flex items-center gap-1 text-xs text-success">
                <Zap size={11} /> streaming
              </span>
            )}
            {size !== null && (
              <span className="text-xs text-muted-foreground font-mono">{formatSize(size)}</span>
            )}
          </div>
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
                {formatSize(phase.size)} — lecture en ligne indisponible sur ce navigateur.
              </p>
              <a href={`/d/${hash}#${keyB64}`} className="text-xs text-primary underline">
                Télécharger et déchiffrer
              </a>
            </div>
          )}
          {phase.kind === "init" && (
            <span className="flex items-center gap-2 text-sm text-muted">
              <Loader2 size={16} className="animate-spin" /> Préparation…
            </span>
          )}
          {phase.kind === "decrypting" && (
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
          {phase.kind === "playing" && (
            <video
              ref={videoRef}
              key={phase.url}
              src={phase.url}
              controls
              autoPlay
              className="w-full h-full"
            />
          )}
        </div>

        <p className="text-xs text-muted-foreground flex items-center justify-center gap-1.5 mt-3 text-center">
          <ShieldCheck size={12} />
          {phase.kind === "playing" && phase.mode === "stream"
            ? "Déchiffré à la volée dans ce navigateur, plage par plage — rien n'est chargé d'avance, y compris quand tu te déplaces dans la vidéo."
            : "Déchiffré dans ce navigateur — le serveur n'a servi que du ciphertext."}
        </p>
      </div>
    </div>
  );
}
