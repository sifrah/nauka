# Couche cluster : placement, healing, rebalancement

## Placement par rendezvous hashing pondéré (WRH)

« Qui doit détenir le shard i de la stripe s du fichier f ? » est une
**fonction pure** de `(file_hash, stripe_idx, shard_idx, vue pondérée)` :

1. Pour la stripe s, chaque nœud reçoit un score
   `-poids / ln(h)` où `h` est un uniforme (0,1) dérivé de
   `blake3(node_id ‖ "\0" ‖ file_hash/s)` et `poids` = sa **capacité disque
   déclarée** (dans l'état Raft, commande `UpdateNodeStats` ; défaut
   100 Gio tant qu'un nœud n'a pas déclaré). Classement par score
   décroissant.
2. Le shard i va au nœud de rang `i mod n`.

Le `ln` est une implémentation maison en opérations IEEE de base
uniquement (+,−,×,÷) : les `ln` de libm varient d'une plateforme à
l'autre, et le placement doit être identique bit à bit sur tous les nœuds.

### Capacité vs durabilité — la sémantique exacte

La probabilité d'être **en tête** du classement est proportionnelle au
poids. Ce que ça implique selon la taille du cluster :

| Taille | Comportement |
|---|---|
| `n > k+m` | chaque stripe choisit ses k+m hébergeurs parmi n : sélection **pleinement proportionnelle** aux capacités, 1 shard/nœud/stripe |
| `k+m ≥ n` | tous les nœuds hébergent chaque stripe ; les poids décident qui prend les shards « supplémentaires » (entre ⌊(k+m)/n⌋ et ⌈(k+m)/n⌉) |
| cas forcé (ex. n=3, 4+2) | anti-affinité stricte 2/2/2 **quels que soient les poids** — concentrer > m shards d'une stripe sur le gros nœud en ferait un point de défaillance unique. Durabilité d'abord, capacité ensuite (choix délibéré, testé) |

Mesuré (4 nœuds, 3×50 Go + 1×350 Go, 288 shards) : 66/63/66/93 — le gros
nœud sature le plafond d'anti-affinité (~33 %), les petits descendent à
~22 %.

La capacité est **déclarée, quasi statique** (taille du filesystem du
data-dir via statvfs, ou `--capacity` explicite), jamais l'espace *libre* :
pondérer par le libre ferait osciller le placement à chaque écriture.
L'équilibre visé est que tous les nœuds se remplissent au même
**pourcentage**. Un changement de capacité (>1 %) est re-déclaré et le
rebalancement suit par scrub+GC, comme tout changement de vue — le WRH
garantit que seuls les shards migrant *vers* le nœud modifié bougent
(testé : doubler un poids déplace ~1/6 des shards, zéro mouvement entre
nœuds inchangés).

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

## Attestation de stockage

`has_shard` est déclaratif : un nœud peut répondre « oui » alors que son
disque a été vidé ou silencieusement corrompu. Deux mécanismes de preuve,
complémentaires, ferment cette faille — et bouclent la promesse du
placement pondéré (capacité *déclarée* → capacité *honorée*).

### 1. Challenge par nonce — utilisé par le GC

`ProveShard { hash, nonce }` : le pair doit renvoyer
`blake3(nonce ‖ octets)`. Le nonce est tiré au hasard à chaque fois :
impossible à pré-calculer ou à rejouer, impossible à produire sans relire
réellement les octets.

Vérifiable seulement par qui détient déjà les octets — c'est exactement la
situation du **GC de rebalancement** : avant de libérer sa copie, un nœud
exige désormais cette preuve de chaque propriétaire actuel (au lieu d'un
simple `has_shard`). Sans preuve, il garde. La redondance ne peut plus
baisser sur une déclaration mensongère.

### 2. Audit par échantillonnage — surveillance continue

En régime permanent, chaque shard n'a qu'**un** détenteur : personne
d'autre n'a les octets pour vérifier un challenge. L'auditeur échantillonne
donc des shards que le pair **possède selon le placement**, les télécharge
et vérifie leur hash contre le manifest. Le stockage étant
content-addressed, tricher reviendrait à produire des octets ayant un
BLAKE3 imposé — une préimage.

Coût borné : `SAMPLE_PER_PEER` (3) shards par pair et par passe de scrub.

Lecture des rapports :

| Champ | Sens |
|---|---|
| `proved` | détention prouvée (hash conforme au manifest) |
| `missing` | le pair ne fournit pas un shard qui lui revient — transitoire si son scrubber est en retard, **alerte si ça persiste** |
| `failed` | octets au mauvais hash : anomalie sérieuse, tracée en `warn` |
| `unreachable` | pair injoignable — pas une faute |

Observé en conditions réelles : cluster sain `6/6 détentions prouvées` →
`rm -rf` des shards d'un nœud → `3/6 prouvées, 3 absentes` → retour à
`6/6` une fois son scrubber ayant tout régénéré.
