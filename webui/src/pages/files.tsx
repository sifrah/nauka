import { useCallback, useEffect, useRef, useState } from "react";
import {
  Copy,
  Download,
  KeyRound,
  Loader2,
  Lock,
  Play,
  RefreshCw,
  Upload as UploadIcon,
} from "lucide-react";
import { toast } from "sonner";
import { useTitle } from "../hooks/use-title";
import { formatSize } from "../lib/format";
import { formatError } from "../lib/errors";
import {
  fetchFiles,
  keyring,
  keyringImport,
  shareLink,
  uploadEncrypted,
  type ClusterFile,
} from "../lib/yog/client";
import { isStreamable } from "../lib/yog/stream";

interface UploadInFlight {
  name: string;
  fraction: number;
}

export function FilesPage() {
  useTitle("Fichiers");
  const [files, setFiles] = useState<ClusterFile[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [uploads, setUploads] = useState<Record<string, UploadInFlight>>({});
  const [dragOver, setDragOver] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const ring = keyring();

  const refresh = useCallback(async () => {
    try {
      setFiles(await fetchFiles());
      setError(null);
    } catch (e) {
      setError(formatError(e));
    }
  }, []);

  useEffect(() => {
    void refresh();
    const t = setInterval(() => void refresh(), 5000);
    return () => clearInterval(t);
  }, [refresh]);

  const startUpload = useCallback(
    async (list: FileList | File[]) => {
      for (const file of Array.from(list)) {
        const id = `${file.name}-${Date.now()}`;
        setUploads((u) => ({ ...u, [id]: { name: file.name, fraction: 0 } }));
        try {
          const result = await uploadEncrypted(file, (fraction) =>
            setUploads((u) => ({ ...u, [id]: { name: file.name, fraction } })),
          );
          await navigator.clipboard.writeText(result.link).catch(() => {});
          toast.success(`${file.name} chiffré et uploadé — lien copié`, {
            description: "La clé est dans le lien (#…), le serveur ne l'a jamais vue.",
          });
        } catch (e) {
          toast.error(`Échec de l'upload de ${file.name}`, { description: formatError(e) });
        } finally {
          setUploads((u) => {
            const next = { ...u };
            delete next[id];
            return next;
          });
          void refresh();
        }
      }
    },
    [refresh],
  );

  const copyLink = (hash: string) => {
    const entry = keyring()[hash];
    if (!entry) return;
    void navigator.clipboard.writeText(shareLink(hash, entry.key));
    toast.success("Lien de partage copié", {
      description: "Quiconque a ce lien peut déchiffrer — le serveur, non.",
    });
  };

  const importKey = (hash: string) => {
    const input = prompt("Coller le lien complet ou la clé (#…) de ce fichier :");
    if (!input) return;
    const key = input.includes("#") ? input.split("#").pop()! : input;
    try {
      keyringImport(hash, key.trim());
      toast.success("Clé importée dans le trousseau local");
      void refresh();
    } catch (e) {
      toast.error("Clé invalide", { description: formatError(e) });
    }
  };

  return (
    <div
      className="p-6 space-y-5 max-w-[1400px] h-full"
      onDragOver={(e) => {
        e.preventDefault();
        setDragOver(true);
      }}
      onDragLeave={() => setDragOver(false)}
      onDrop={(e) => {
        e.preventDefault();
        setDragOver(false);
        if (e.dataTransfer.files.length) void startUpload(e.dataTransfer.files);
      }}
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold">Fichiers</h1>
          <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Lock size={12} /> chiffrés de bout en bout — le cluster ne peut pas les lire
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => void refresh()}
            className="flex items-center justify-center w-8 h-8 rounded-lg text-muted-foreground hover:text-foreground hover:bg-accent transition-colors"
            title="Rafraîchir"
          >
            <RefreshCw size={15} strokeWidth={1.5} />
          </button>
          <button
            onClick={() => inputRef.current?.click()}
            className="flex items-center gap-2 text-sm bg-accent hover:bg-border-bright border border-border-bright rounded-lg px-3 py-1.5 transition-colors"
          >
            <UploadIcon size={15} strokeWidth={1.5} /> Uploader
          </button>
          <input
            ref={inputRef}
            type="file"
            multiple
            className="hidden"
            onChange={(e) => e.target.files && void startUpload(e.target.files)}
          />
        </div>
      </div>

      {Object.entries(uploads).map(([id, u]) => (
        <div key={id} className="card-surface rounded-lg p-4">
          <div className="flex items-center justify-between text-sm mb-2">
            <span className="flex items-center gap-2">
              <Loader2 size={14} className="animate-spin" /> Chiffrement + upload de {u.name}
            </span>
            <span className="font-mono text-muted-foreground">{Math.round(u.fraction * 100)}%</span>
          </div>
          <div className="h-1.5 bg-accent rounded-full overflow-hidden">
            <div
              className="h-full bg-primary transition-all"
              style={{ width: `${u.fraction * 100}%` }}
            />
          </div>
        </div>
      ))}

      {error && (
        <div className="card-surface rounded-lg p-5">
          <p className="text-destructive text-sm font-mono">{error}</p>
        </div>
      )}

      {files === null && !error && (
        <div className="flex items-center justify-center py-16 text-muted">
          <Loader2 size={18} className="animate-spin" />
        </div>
      )}

      {files !== null && files.length === 0 && Object.keys(uploads).length === 0 && (
        <div
          className={`card-surface rounded-lg p-16 text-center text-muted-foreground border-2 border-dashed ${dragOver ? "border-primary" : "border-transparent"}`}
        >
          <UploadIcon size={24} className="mx-auto mb-3 opacity-60" />
          <p className="text-sm">Glisser un fichier ici, ou cliquer sur « Uploader ».</p>
          <p className="text-xs mt-2">
            Chiffré dans le navigateur avant d'être envoyé — la clé reste chez toi.
          </p>
        </div>
      )}

      {files !== null && files.length > 0 && (
        <div className="card-surface rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs uppercase tracking-wider text-muted-foreground border-b border-border">
                <th className="px-4 py-3 font-medium">Fichier</th>
                <th className="px-4 py-3 font-medium">Taille</th>
                <th className="px-4 py-3 font-medium">Hash</th>
                <th className="px-4 py-3 font-medium text-right">Actions</th>
              </tr>
            </thead>
            <tbody>
              {files.map((f) => {
                const entry = ring[f.hash];
                return (
                  <tr key={f.hash} className="border-b border-border/50 hover:bg-accent/40">
                    <td className="px-4 py-2.5">
                      {entry?.name || f.name || (
                        <span className="flex items-center gap-1.5 text-muted-foreground">
                          <Lock size={12} /> (nom chiffré)
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-2.5 font-mono tabular-nums text-muted-foreground">
                      {formatSize(f.size)}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground">
                      {f.hash.slice(0, 16)}…
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center justify-end gap-1">
                        {entry ? (
                          <>
                            {isStreamable(entry.name) && (
                              <a
                                href={`/w/${f.hash}#${entry.key}`}
                                className="flex items-center justify-center w-7 h-7 rounded-md text-muted-foreground hover:text-foreground hover:bg-accent"
                                title="Lire (déchiffré dans le navigateur, seek compris)"
                              >
                                <Play size={14} strokeWidth={1.5} />
                              </a>
                            )}
                            <a
                              href={`/d/${f.hash}#${entry.key}`}
                              className="flex items-center justify-center w-7 h-7 rounded-md text-muted-foreground hover:text-foreground hover:bg-accent"
                              title="Télécharger (déchiffré localement)"
                            >
                              <Download size={14} strokeWidth={1.5} />
                            </a>
                            <button
                              onClick={() => copyLink(f.hash)}
                              className="flex items-center justify-center w-7 h-7 rounded-md text-muted-foreground hover:text-foreground hover:bg-accent"
                              title="Copier le lien de partage"
                            >
                              <Copy size={14} strokeWidth={1.5} />
                            </button>
                          </>
                        ) : (
                          <button
                            onClick={() => importKey(f.hash)}
                            className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground px-2 py-1 rounded-md hover:bg-accent"
                            title="Ce navigateur n'a pas la clé — l'importer depuis un lien"
                          >
                            <KeyRound size={12} /> importer la clé
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
