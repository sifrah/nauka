import { useEffect, useState } from "react";
import { Crown, Database, Files, HardDrive, Loader2, Server } from "lucide-react";
import { useTitle } from "../hooks/use-title";
import { formatSize } from "../lib/format";
import { formatError } from "../lib/errors";
import { StatCard } from "../components/dashboard/StatCard";
import { fetchStatus, type ClusterStatus } from "../lib/yog/client";

export function DashboardPage() {
  useTitle("Cluster");
  const [status, setStatus] = useState<ClusterStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    const tick = async () => {
      try {
        const s = await fetchStatus();
        if (live) {
          setStatus(s);
          setError(null);
        }
      } catch (e) {
        if (live) setError(formatError(e));
      }
    };
    void tick();
    const t = setInterval(() => void tick(), 2000);
    return () => {
      live = false;
      clearInterval(t);
    };
  }, []);

  if (error) {
    return (
      <div className="p-6">
        <div className="card-surface rounded-lg p-5">
          <p className="text-destructive text-sm font-mono">{error}</p>
        </div>
      </div>
    );
  }

  if (!status) {
    return (
      <div className="flex flex-col items-center justify-center h-full gap-3 text-muted">
        <Loader2 size={20} className="animate-spin" />
        <span className="text-sm">Connexion au cluster…</span>
      </div>
    );
  }

  const totalCapacity = status.nodes.reduce((sum, n) => sum + n.capacity_bytes, 0);

  return (
    <div className="p-6 space-y-5 max-w-[1400px] overflow-hidden">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold">Cluster</h1>
          <span className="flex items-center gap-1.5 text-xs text-success bg-success/10 px-2 py-0.5 rounded-full">
            <span className="w-1.5 h-1.5 rounded-full bg-success animate-pulse" />
            Live
          </span>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          title="Nœuds"
          value={String(status.nodes.length)}
          icon={<Server size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Fichiers"
          value={status.files.toLocaleString()}
          icon={<Files size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Stocké (chiffré)"
          value={formatSize(status.total_bytes)}
          icon={<Database size={18} strokeWidth={1.5} />}
        />
        <StatCard
          title="Capacité déclarée"
          value={formatSize(totalCapacity)}
          icon={<HardDrive size={18} strokeWidth={1.5} />}
        />
      </div>

      <div className="card-surface rounded-lg overflow-hidden">
        <div className="px-5 pt-4 pb-2">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Membres
          </p>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs uppercase tracking-wider text-muted-foreground border-b border-border">
              <th className="px-5 py-2 font-medium">Adresse</th>
              <th className="px-5 py-2 font-medium">Capacité</th>
              <th className="px-5 py-2 font-medium">Part du placement</th>
              <th className="px-5 py-2 font-medium text-right">Rôle</th>
            </tr>
          </thead>
          <tbody>
            {status.nodes.map((n) => (
              <tr key={n.addr} className="border-b border-border/50">
                <td className="px-5 py-2.5 font-mono text-xs">
                  {n.addr}
                  {n.is_self && <span className="text-muted-foreground"> (ce nœud)</span>}
                </td>
                <td className="px-5 py-2.5 font-mono tabular-nums">
                  {formatSize(n.capacity_bytes)}
                </td>
                <td className="px-5 py-2.5 w-1/3">
                  <div className="h-1.5 bg-accent rounded-full overflow-hidden">
                    <div
                      className="h-full bg-primary/70"
                      style={{
                        width: `${totalCapacity ? (n.capacity_bytes / totalCapacity) * 100 : 0}%`,
                      }}
                    />
                  </div>
                </td>
                <td className="px-5 py-2.5 text-right">
                  {n.is_leader ? (
                    <span className="inline-flex items-center gap-1 text-xs text-warning">
                      <Crown size={12} /> leader
                    </span>
                  ) : (
                    <span className="text-xs text-muted-foreground">votant</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p className="text-xs text-muted-foreground">
        Chaque fichier est chiffré côté client puis découpé en shards Reed-Solomon dispersés sur
        les nœuds — le cluster répare et rééquilibre en continu, sans pouvoir lire quoi que ce
        soit.
      </p>
    </div>
  );
}
