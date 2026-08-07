# Décisions structurantes et leçons

Journal des choix qui ont façonné le système, avec leurs raisons — et les
leçons payées en debugging. Utile avant de proposer un changement : la
plupart des « pourquoi pas X ? » ont déjà une réponse ici.

## Choix d'architecture

**Reed-Solomon par stripes plutôt que réplication.** 4+2 = survie à 2
pertes pour +50 % de stockage, là où une réplication ×3 coûte +200 % pour
la même tolérance. Le découpage en stripes (4 Mio) permet le streaming et
borne la mémoire.

**Raft embarqué (openraft) plutôt que gossip ou coordinateur central.**
Métadonnées fortement cohérentes, pas de dépendance externe, tolère la
minorité en panne. Le gossip aurait été plus simple mais eventual-consistent
sur le placement — inacceptable pour un registre de fichiers.

**QUIC (quinn) plutôt que gRPC/TCP.** Multiplexage natif des streams
(des centaines de shards en parallèle sur une connexion), 0-RTT de reprise,
chiffrement TLS intégré, et un seul protocole pour data + consensus +
admin. Prix payé : le tuning (voir leçons).

**Rendezvous hashing plutôt que consistent hashing en anneau.** Pas de
table à répliquer, pas de vnodes, anti-affinité par stripe naturelle, et
un changement de vue ne déplace que le strict nécessaire.

**WRH pondéré par la capacité TOTALE déclarée, jamais par l'espace libre.**
Pondérer par le libre ferait dépendre le placement de ce qu'on vient de
placer → oscillations sans fin. Avec la capacité totale, l'équilibre est
« même pourcentage de remplissage partout ». Les poids vivent dans l'état
Raft (vue partagée obligatoire : un placement calculé sur des mesures
locales divergentes ferait se contredire scrub et GC). Et le `ln` du score
est implémenté en opérations IEEE de base : les libm diffèrent entre
plateformes, or deux nœuds qui classent différemment se disputent les
shards. Enfin : l'anti-affinité prime sur la capacité quand elles sont en
conflit (petit cluster) — un gros nœud qui concentrerait plus de m shards
d'une stripe deviendrait un point de défaillance unique.

**Mainline DHT + pkarr plutôt qu'IPFS pour le rendez-vous.** L'héritage
ChainRage sans sa stack : kubo est en Go (inembarquable), rust-ipfs est
mort, et IPFS est surdimensionné pour publier ~200 octets d'adresses. La
DHT BitTorrent est plus vieille, plus grosse, plus fiable — et pkarr y met
des records DNS signés Ed25519. La keypair DHT est **dérivée de la clé de
cluster** : rien à distribuer de plus, pas même une URL.

**Élection de genèse plutôt qu'un flag `--bootstrap`.** Aucun nœud désigné :
candidatures signées sur la DHT, le plus petit node-id fonde après 12 s
d'incontestation, un candidat mort est remplacé après 45 s. La même
commande partout est un choix produit (« clé en main ») autant que
technique.

**Le node-id est dérivé de la clé publique.** `u64 = blake3(pubkey)[..8]`.
Une identité qui se prouve (mTLS) et se calcule, au lieu d'un entier
décrété. Idée reprise de l'UUIDv8 de ChainRage — sans la partie géographie,
qui attendra le placement par région.

**fsync : oui pour le consensus, non pour les shards.** Un vote ou une
entrée Raft acquittés doivent survivre au crash (correction de Raft). Un
shard perdu sur crash machine, lui, est exactement ce que le scrubber sait
réparer — et le fsync par shard divisait l'ingestion par ~20.

## Leçons payées (chronologie des stress tests)

1. **`cargo test --release` ne rebuilde pas les binaires.** Une heure de
   perf-debugging sur un binaire stale. Toujours `cargo build --release
   -p yog-node` avant une démo.
2. **Le MTU quinn est plafonné par `max_udp_payload_size`** (1472 o par
   défaut), pas seulement par `initial_mtu`/la découverte. C'était LE
   goulot : 6 → 83 Mo/s en le levant. Les stats de chemin
   (`Connection::stats()`) ont été décisives pour le voir (`mtu=1472`).
3. **Cubic s'effondre sur les liens rapides à petit buffer** (5 495 pertes,
   RTT 526 ms de bufferbloat, MTU black-holé). BBR pace et tient le débit.
4. **macOS : buffer d'envoi UDP par défaut = 9216 octets.** Toujours
   dimensionner les sockets soi-même.
5. **Le plan de données affame le consensus** s'ils partagent un socket :
   heartbeats en timeout, ré-élection en pleine rafale de 15 Go. D'où le
   plan QUIC dédié (port+1, petits buffers = délai borné) + le test de
   régression qui inonde et vérifie zéro bascule.
6. **Une collision de ports peut être silencieuse.** Le nœud 2 mourait au
   bind pendant que son trafic était absorbé par le plan consensus du
   nœud 1 (qui servait aussi le protocole data). Deux garde-fous : le plan
   consensus ne sert QUE du Raft, et `cluster-init` fait un pre-flight
   des deux plans avec vérification du node-id qui répond.
7. **Sans timeout ni mémoire d'échec, un nœud mort bloque tout.** Le
   download API retentait une connexion vers le nœud disparu à chaque
   shard. Règle : timeout partout (connexion 3 s, shard 20 s), et un pair
   en échec est marqué pour la durée de la requête.
8. **Une asymétrie de sérialisation se paie au restart.** `purge` écrivait
   `Some(LogId)` là où le démarrage relisait un `LogId` nu → index fantôme
   (24618) → crash au premier redémarrage après purge. Les tests de
   persistance à double coupure l'ont attrapé.
9. **Les échecs doivent être francs.** Keep-alive + idle timeout explicites
   partout : une connexion qui traîne vaut pire qu'une connexion morte,
   parce que les retries idempotents savent gérer la seconde.

## Dettes assumées

Consolidées et priorisées dans [backlog.md](backlog.md), avec les pistes
d'innovation.
