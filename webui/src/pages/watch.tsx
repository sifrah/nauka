// Encrypted media player: /w/<hash>#<key>.
//
// Nominal mode — STREAMING: a Service Worker decrypts on the fly, range by
// range. Nothing is loaded ahead of time: playback starts immediately and a
// seek costs a single round trip, whatever the file size. The server only
// ever serves ciphertext.
//
// Fallback — if playback does not start (worker unavailable, restrictive
// browser): full decryption in memory, then a Blob URL. Simple and robust,
// but the whole file has to be waited for, hence the size cap.

import { useEffect, useRef, useState } from "react";
import { useParams } from "react-router";
import { Loader2, Lock, ShieldCheck, TriangleAlert, Zap } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { formatSize } from "../lib/format";
import { formatError } from "../lib/errors";
import { decodeKey, decryptStream } from "../lib/yog/crypto";
import { keyring, keyringImport } from "../lib/yog/client";
import { guessMime, plainSizeFromCipher, prepareStream } from "../lib/yog/stream";

/** Beyond this, the "everything in memory" fallback is unreasonable. */
const MAX_FALLBACK = 600 * 1024 * 1024;
/** Grace period for streaming to produce readable metadata. */
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
  useTitle(name ?? "Playback");
  const [phase, setPhase] = useState<Phase>({ kind: "init" });
  const [size, setSize] = useState<number | null>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const blobUrlRef = useRef<string | null>(null);
  const keyB64 = location.hash.slice(1) || entry?.key || "";

  useEffect(() => {
    let cancelled = false;

    /** Fallback: decrypt everything, then a Blob URL. */
    const fallback = async (key: Uint8Array, plainSize: number) => {
      if (plainSize > MAX_FALLBACK) {
        setPhase({ kind: "too-big", size: plainSize });
        return;
      }
      setPhase({ kind: "decrypting", done: 0, total: plainSize });
      const resp = await fetch(`/f/${hash}`);
      if (!resp.ok || !resp.body) throw new Error(`download rejected (${resp.status})`);
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
        if (!keyB64) throw new Error("link without key (#…) — cannot play");
        const key = decodeKey(keyB64);
        const head = await fetch(`/f/${hash}`, { method: "HEAD" });
        if (!head.ok) throw new Error(`file not found (${head.status})`);
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

        // Did streaming actually start? If not, fall back silently.
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
            <h1 className="text-sm font-medium">{name ?? "Encrypted media"}</h1>
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
                {formatSize(phase.size)} — in-browser playback unavailable on this browser.
              </p>
              <a href={`/d/${hash}#${keyB64}`} className="text-xs text-primary underline">
                Download and decrypt
              </a>
            </div>
          )}
          {phase.kind === "init" && (
            <span className="flex items-center gap-2 text-sm text-muted">
              <Loader2 size={16} className="animate-spin" /> Preparing…
            </span>
          )}
          {phase.kind === "decrypting" && (
            <div className="w-2/3 space-y-3 text-center">
              <span className="flex items-center justify-center gap-2 text-sm text-muted">
                <Loader2 size={16} className="animate-spin" /> Decrypting…
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
            ? "Decrypted on the fly in this browser, range by range — nothing is loaded ahead of time, including when you seek through the video."
            : "Decrypted in this browser — the server only ever served ciphertext."}
        </p>
      </div>
    </div>
  );
}
