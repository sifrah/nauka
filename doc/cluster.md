# Couche cluster : placement, healing, rebalancement

## Placement par rendezvous hashing (HRW)

« Qui doit détenir le shard i de la stripe s du fichier f ? » est une
**fonction pure** de `(file_hash, stripe_idx, shard_idx, membres triés)` :

1. Pour la stripe s, les nœuds sont classés par
   `blake3(node_id ‖ "\0" ‖ file_hash/s)` décroissant (Highest Random
   Weight).
2. Le shard i va au nœud de rang `i mod n`.

Propriétés :

- **Zéro coordination** : tous les nœuds calculent le même placement à
  partir de la même vue (le membership Raft, trié).
- **Anti-affinité** : les shards d'une même stripe tombent sur des nœuds
  distincts dès que `n ≥ k+m` — la perte d'un nœud coûte au plus 1 shard
  par stripe (avec 3 nœuds et 4+2 : 2 shards par stripe, toujours ≤ m).
- **Étalement** : le classement change de stripe en stripe → charge
  uniforme (mesuré : 16/16/16 sur 3 nœuds, 12/12/12/12 sur 4).
- **Stabilité incrémentale** : ajouter/retirer un nœud ne déplace que les
  shards strictement nécessaires (propriété du HRW), pas tout le cluster.

## Auto-healing (scrub)

Chaque nœud vérifie périodiquement les shards **dont il est propriétaire** :

```
pour chaque manifest connu localement :
  pour chaque (stripe, shard) dont owner == moi :
    get_shard local OK ?           → rien à faire
    manquant OU corrompu (hash) ?  → réparation :
      collecte des shards de la stripe (local puis pairs, propriétaire
      théorique en premier) jusqu'à ≥ k valides
      decode_stripe → ré-encode_stripe → le shard régénéré doit matcher
      le hash du manifest → stocké
```

Rapport par passe : `shards_checked / healed / unrecoverable`. Un shard
irréparable (moins de k survivants *pour l'instant*) sera retenté à la
passe suivante — des nœuds peuvent revenir.

## GC de rebalancement

Le pendant « libération » du scrub, pour les changements de topologie :

```
pour chaque shard local :
  référencé par aucun manifest      → ignoré (orphelin, hors périmètre v1)
  dont je suis propriétaire         → gardé
  sinon : TOUS ses propriétaires actuels (un shard peut être partagé par
  plusieurs fichiers) confirment le détenir (has_shard) ?
    oui → suppression locale
    non (ou injoignable) → gardé, retenté plus tard
```

La règle « tous confirment, sinon on garde » garantit qu'on ne réduit
jamais la redondance réelle du cluster en libérant trop tôt.

## Membership à chaud

- **`cluster-add id@addr`** : le nœud entre en **learner** (rattrape le log
  et le snapshot sans droit de vote), puis est **promu votant**. Le
  rebalancement suit automatiquement au fil des scrubs/GC.
- **`cluster-remove id`** : le nœud sort du membership mais **reste allumé
  pendant le drain** — il sert encore les lectures pendant que les autres
  re-répliquent sa part. On l'éteint ensuite.
- En mode découverte, `cluster-add` est automatique (auto-join, voir
  [identite-et-decouverte.md](identite-et-decouverte.md)).

Séquence mesurée en réel : 3 nœuds à 16/16/16 shards → `cluster-add` d'un
4ᵉ → 12/12/12/12 en quelques cycles → `cluster-remove` du 3ᵉ → 16/16/16
sur les survivants → extinction du retiré → fichier re-téléchargé intact.

## Mode statique (sans consensus)

`serve --peers a,b,c` sans `--node-id` : vue du cluster figée en config,
heartbeats + scrub périodiques, pas de registre répliqué (les manifests
sont répliqués sur tous les nœuds à l'upload par `put-remote`). Conservé
pour les déploiements minimalistes et les tests ; le mode consensus est le
mode nominal.
