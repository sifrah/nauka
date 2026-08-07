# Opérations

## Déploiement type (N VPS)

```bash
# 1. Sur ton poste — générer la clé du cluster, UNE fois :
nauka keygen --out ./nauka-keys
scp -r nauka-keys vps1:/etc/nauka-keys   # idem vps2, vps3…

# 2. Sur CHAQUE VPS — la même commande :
nauka --data-dir /var/lib/nauka --keys /etc/nauka-keys serve
```

C'est tout. Chaque nœud dérive son identité, détecte son IP publique,
trouve le cluster sur la DHT (ou le fonde s'il est le premier), adhère,
et participe au stockage/healing. Ordre de démarrage indifférent.

**Firewall — le point qui piège tout le monde :** ouvrir en **UDP** le port
d'écoute ET le suivant (défaut : `7311/udp` + `7312/udp`), plus le port
HTTP en TCP (défaut `8080/tcp`). Tout le trafic inter-nœuds est QUIC,
donc UDP. Plusieurs nœuds sur un même hôte : espacer les ports d'au
moins 2 (le pre-flight de `cluster-init` détecte les collisions).

## Référence CLI

Options globales : `--data-dir <dir>` (défaut `./nauka-data`),
`--keys <dir>` (active mTLS + identité dérivée).

| Commande | Rôle |
|---|---|
| `keygen --out <dir>` | génère la clé de cluster (refuse d'écraser) |
| `node-info` | node-id + fingerprint de ce nœud (requiert `--keys`) |
| `serve` | démarre le nœud (voir options ci-dessous) |
| `put <fichier>` / `get <hash> -o f` / `verify <hash>` / `list` | opérations locales (sans réseau) |
| `put-remote <fichier> --peers a,b,c` | encode + dispatche depuis la machine cliente |
| `get-remote <hash> --peers a,b,c -o f` | reconstruit depuis les peers joignables |
| `cluster-init <id@addr>…` | initialise un cluster (mode manuel ; pre-flight des deux plans) |
| `cluster-add <id@addr> --peers …` | ajout à chaud (learner → votant) |
| `cluster-remove <id> --peers …` | retrait à chaud (drain par les scrubs) |
| `cluster-metrics --peer <addr>` | leader, membres, index appliqué |

Options de `serve` :

| Option | Défaut | Rôle |
|---|---|---|
| `--listen` | `0.0.0.0:7311` | socket QUIC data (consensus = port+1) |
| `--advertise` | auto-détecté (DHT) sinon `--listen` | adresse annoncée aux autres |
| `--http` / `--no-http` | `0.0.0.0:8080` | API HTTP publique |
| `--scrub-interval` | `30` s | cadence healing + GC |
| `--capacity` | taille du filesystem du data-dir | poids du placement pondéré, en octets |
| `--no-discover` | — | désactive la DHT (statique/air-gapped) |
| `--peers a,b,c` | — | mode statique (désactive la DHT) |
| `--node-id` | dérivé des clés | id Raft manuel (mode sans clés uniquement) |

## Santé et diagnostic

- `cluster-metrics --peer <addr>` : le leader est-il élu ? tous les membres
  sont-ils là ? l'index appliqué progresse-t-il ?
- Logs du nœud : `scrub: X vérifiés, Y régénérés, Z irréparables` (un Y > 0
  signale une réparation réelle ; un Z persistant signale trop de nœuds
  morts), `gc: N shards libérés` (rebalancement), warnings
  `peer … injoignable`.
- `verify <hash>` (local) : le fichier est-il reconstructible avec ce que
  ce nœud voit ?
- L'API `/api/files` doit rendre la même liste sur tous les nœuds (modulo
  quelques centaines de ms de réplication).

## Sauvegarde / restauration

- **À sauvegarder** : le dossier de clés (`cluster-ca.key` surtout — sa
  perte interdit tout nouveau nœud et toute nouvelle machine cliente), et
  idéalement les `node.key` (sinon un nœud réinstallé prend une nouvelle
  identité, l'ancienne se retire avec `cluster-remove`).
- **Les data-dirs se reconstruisent** : un nœud au disque vierge qui
  redémarre avec ses clés ré-adhère et le healing lui redonne sa part.
  (Ne pas vider plus de m nœuds à la fois !)
- Un arrêt total du cluster (coupure électrique) est couvert : tout l'état
  nécessaire est durable dans les data-dirs.

## Limites connues (v1)

| Limite | Contournement / plan |
|---|---|
| Pas de traversée de NAT (hole punching/relais) | nœuds avec IP publique ou port forwardé ; relais à venir |
| `put-remote`/`get-remote` exigent `--peers` explicites | passer par l'API HTTP, ou lire les adresses via `cluster-metrics` |
| Pas de DELETE/expiration côté API ; GC des shards orphelins non fait | à venir avec la purge de registre |
| API HTTP sans authentification ni quotas | reverse proxy en attendant |
| Clé de cluster présente sur chaque nœud | émission de certificats hors-ligne à venir |
| Partage de bande passante inéquitable entre uploads concurrents (les gros flux dominent) | sans danger — fair queuing en backlog |
| Fenêtre ≤ 2 min de republication DHT après bascule de leader | n'affecte que les nouveaux arrivants pendant la fenêtre |
| À n ≤ k+m nœuds, la capacité ne peut pas primer sur l'anti-affinité (voir cluster.md) | ajouter des nœuds, ou accepter que le petit disque limite |
| Pas de refus d'écriture sur disque plein (garde-fou ~95 %) | surveiller le remplissage ; garde-fou à venir |
