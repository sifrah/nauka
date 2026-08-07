# Identité cryptographique et découverte

## La clé de cluster

```
nauka keygen --out ./nauka-keys
  → nauka-keys/cluster-ca.key   (CA Ed25519, permissions 0600 — LE secret)
  → nauka-keys/cluster-ca.pem   (certificat racine)
```

**Posséder ce dossier = appartenir au cluster.** C'est l'unique chose à
distribuer aux machines (scp). Tout le reste se dérive :

| Dérivé | Comment |
|---|---|
| Identité du nœud | keypair Ed25519 auto-générée (`data-dir/node.key`, 0600), certificat signé par la CA au démarrage |
| **node-id Raft** | `u64` = 8 premiers octets de `blake3(pubkey du nœud)` — l'identité se prouve, elle ne se décrète pas (`--node-id` est ignoré avec warning si contradictoire) |
| Fingerprint | `blake3(pubkey)` hex complet (affiché par `node-info`) |
| Identité DHT du cluster | keypair pkarr = `blake3("nauka-discovery-v1" ‖ clé CA)` — déterministe : tous les détenteurs des clés publient/résolvent au même endroit |

## mTLS

Sur les **deux plans QUIC** (data et consensus) :

- le serveur exige un certificat client **signé par la clé de cluster** ;
- le client vérifie le serveur contre la CA (SNI `node.nauka`).

Un client sans certificat, ou porteur d'un certificat d'un *autre*
cluster, meurt au handshake (testé). Les commandes CLI (`put-remote`,
`cluster-metrics`…) s'authentifient avec une identité éphémère signée par
la même CA (`--keys` global).

**Limite v1 assumée** : la clé de cluster est distribuée à tous les nœuds —
n'importe quel détenteur peut émettre des certificats. Blast radius
identique à un secret partagé, mais le lien est réellement authentifié et
chiffré. Étape suivante naturelle : émission de certificats hors-ligne par
nœud (la CA ne quitte plus le poste d'admin).

## Découverte via la DHT Mainline (pkarr)

Aucune infrastructure : le cluster publie un **enregistrement DNS signé**
(records TXT) sous sa clé pkarr, directement dans la DHT BitTorrent
Mainline (~10 M de nœuds, 20 ans d'ancienneté).

- `_seeds` : jusqu'à 8 adresses de membres (n'importe quel seed joignable
  suffit — le membership complet vient ensuite du cluster lui-même).
  Republication par le **leader toutes les 2 min** (battement de cœur —
  les records DHT s'évaporent naturellement).
- `_genesis` : candidature de fondation (voir plus bas). Écrasée par la
  publication des seeds (même record pkarr).

Ce qui est public : les adresses IP des seeds (stockées sur des nœuds
BitTorrent tiers) — mais introuvables sans la clé publique du cluster, et
inutilisables sans certificat. Les données ne touchent jamais la DHT.

**Découverte ≠ admission** : la DHT localise, le mTLS autorise.

## Cycle de vie d'un nœud (`serve` avec `--keys`, tout implicite)

```
membre déjà (état Raft durable) ─────────────────────────▶ servir
sinon, boucle (5 s) :
  _seeds non vide ?  → adhésion: AddLearner puis promotion votant
                       via le leader (redirections suivies) → servir
  _seeds vide :
    _genesis :
      candidat avec id < le mien  → je m'incline (mais s'il ne fonde
                                     jamais : déclaré mort après 45 s,
                                     je reprends la main)
      candidat avec id > le mien  → je (re)publie ma candidature
      ma candidature, incontestée
      depuis ≥ 12 s               → JE FONDE le cluster (mono-membre)
      aucune candidature          → je publie la mienne
```

Le plus petit node-id gagne la genèse — déterministe, sans nœud désigné,
sans flag. Validé sur la vraie Mainline avec deux nœuds démarrés
simultanément sur une clé vierge : un seul cluster émerge.

Fenêtre de split-brain résiduelle : deux candidatures simultanées dont
aucune ne se propage sur la DHT pendant 12 s — improbable, et ne concerne
que la toute première minute de vie d'un cluster, jamais un cluster établi.

## Auto-détection de l'IP publique

Sans `--advertise`, le nœud demande son adresse… à la DHT elle-même : les
nœuds Mainline renvoient l'adresse d'où ils nous voient (BEP42) et le
client en fait un consensus. Aucun service tiers (pas d'ipify). Repli sur
l'adresse d'écoute avec message explicite si la DHT n'a pas convergé.
L'adresse détectée n'est joignable que si les ports UDP sont ouverts —
c'est rappelé dans le log de démarrage.

## Modes réseau, en résumé

| Commande | Comportement |
|---|---|
| `serve --keys k` | **le mode nominal** : mTLS + node-id dérivé + découverte DHT + genèse + IP auto |
| `serve --keys k --no-discover` | idem sans DHT (cluster statique/air-gapped, init manuelle) |
| `serve --keys k --peers a,b` | mode statique mTLS (la présence de `--peers` désactive la DHT) |
| `serve` (sans clés) | mode legacy insecure (dev uniquement, warning) |
