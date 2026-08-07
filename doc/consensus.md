# Consensus Raft

## Ce que Raft réplique (et ce qu'il ne réplique pas)

La state machine répliquée contient **uniquement des métadonnées** :

- le **registre des fichiers** : `file_hash → FileManifest` (commandes
  `RegisterManifest` / `UnregisterManifest`) ;
- le **membership** du cluster (géré nativement par openraft).

Les octets des shards ne passent **jamais** par le log de consensus — ils
voyagent en direct par le plan data QUIC. Un manifest pèse quelques Kio
quel que soit le fichier : le consensus reste léger à n'importe quelle
échelle de stockage.

Le registre répliqué est la **source de vérité** : chaque nœud matérialise
localement les manifests qu'il y découvre, puis son scrubber va chercher
les shards qui lui reviennent. Un nœud qui a raté un upload converge tout
seul.

## Paramètres openraft

```
heartbeat_interval        500 ms
election_timeout          1,5 – 3 s
snapshot_policy           LogsSinceLast(256)
max_in_snapshot_log_to_keep  64
```

Réseau : les RPCs (`append_entries`, `vote`, `install_snapshot`) sont
transportées par notre QUIC, sur le **plan consensus dédié (port+1)** —
voir [transport.md](transport.md).

## Persistance (data-dir/raft/)

| Élément | Support | Durabilité |
|---|---|---|
| Log + vote + committed + last_purged | `raft-log.redb` (redb) | **fsync avant l'ack** — exigence de correction de Raft : un vote ou une entrée acquittés doivent survivre au crash |
| State machine (registre) | mémoire | reconstruite au démarrage : snapshot + replay du log par openraft — **aucun fsync sur le chemin d'apply** |
| Snapshot | `snapshot.bin` | écriture atomique (tmp + fsync + rename) |

Le log redb reste borné : snapshot tous les 256 entrées puis purge (en
gardant 64 entrées de marge pour que les followers en retard rattrapent par
le log plutôt que par snapshot complet).

Scénarios couverts par les tests :

- **Crash du leader en plein trafic** → ré-élection ~2 s, reprise des
  écritures, zéro perte de ce qui était committé.
- **Résurrection à état vide** (disque perdu) → rattrapage complet depuis
  le leader (snapshot + log).
- **Coupure totale du cluster** (les n nœuds éteints, `kill -9` compris) →
  redémarrage depuis les data-dirs, registre intact, cluster à nouveau
  écrivable. Testé en pur replay de log ET en snapshot+purge+reliquat.

## Écritures et administration

Toute écriture passe par le leader. Deux chemins :

- **Côté nœud** : `RaftApp::write(cmd)` — `client_write` local si leader,
  sinon transmission au leader via le transport (utilisé par l'API HTTP).
- **Côté client CLI** : `admin_via_leader(peers, req)` — essaie chaque
  peer, suit les redirections `ForwardTo`, retente pendant les bascules.

RPCs d'admin (portées par `RaftRpc::Admin`) :

```
Init(members)                  initialisation du cluster (une fois)
AddLearner { id, addr }        ajout en learner (rattrape sans voter)
ChangeMembership([ids])        changement de l'ensemble des votants
Write(cmd)                     écriture dans le registre
Metrics                        id, leader, membres, index appliqué
ListManifests                  clés du registre
```

## Performances mesurées

- ~1 300–1 500 écritures/s dans le registre (32 writers concurrents, build
  debug, avant persistance redb — la version durable paie un fsync par
  batch d'append, amorti par le batching d'openraft).
- 500 écritures concurrentes convergées sur 3 nœuds : voir
  `nauka-raft/tests/stress.rs`.
