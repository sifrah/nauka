# Chiffrement de bout en bout

**Les nœuds stockent et servent des octets qu'ils ne peuvent pas lire.**
Le fichier est chiffré côté client AVANT le découpage Reed-Solomon ; le
serveur sharde, disperse, répare et sert du ciphertext, sans jamais voir
ni le contenu, ni la clé, ni (par défaut) le nom du fichier.

## Utilisation

```
# chiffre localement puis uploade — imprime le lien complet :
nauka-node upload plans.pdf --api http://node1:8080
→ http://node1:8080/f/4fae2bb2…#RO_5yMPbAwtIn0kl1UVHQeG…

# télécharge + déchiffre + vérifie (le lien complet, avec le #…) :
nauka-node download "http://node3:8080/f/4fae2bb2…#RO_5yMPb…" -o plans.pdf
```

Le lien fonctionne depuis **n'importe quel nœud** (changer l'hôte suffit,
le hash et la clé restent les mêmes).

## Pourquoi le fragment (#) est le bon endroit pour la clé

Par construction du protocole HTTP, **le fragment n'est jamais envoyé au
serveur** — ni dans la requête, ni dans les logs, ni aux proxys. Quiconque
possède le lien complet peut déchiffrer ; quiconque n'a que le hash (les
nœuds, un espion du registre) ne peut rien. C'est le modèle « le lien EST
la capacité », popularisé par Mega et Firefox Send.

## Schéma cryptographique

- **Clé** : 32 octets aléatoires par fichier, base64url dans le fragment.
- **AES-256-GCM en chunks de 1 Mio** (construction STREAM) :
  nonce = préfixe aléatoire (8 o) ‖ compteur big-endian (4 o) ; le flag
  « dernier chunk » est dans les données authentifiées (AAD).
  Conséquence : modification, troncature, réordonnancement et ajout de
  données sont tous détectés — pas seulement l'altération d'octets.
- **Pourquoi AES-GCM et pas XChaCha20** : c'est le seul AEAD natif de
  WebCrypto — la future UI web pourra déchiffrer dans le navigateur sans
  bibliothèque wasm. (AES-NI/ARMv8-crypto le rendent rapide partout.)
- Formats : en-tête `"YGE1" ‖ préfixe(8)`, puis par chunk
  `longueur u32 LE ‖ flags u8 ‖ ciphertext(+tag 16 o)`.
  Surcoût total : ~16 o/Mio + 12 o d'en-tête (~0,002 %).

Le hash BLAKE3 côté serveur (intégrité des shards, healing, dédup) porte
sur le **ciphertext** — les deux couches d'intégrité sont indépendantes :
le cluster prouve qu'il rend les octets qu'il a reçus, l'AEAD prouve
qu'ils sont bien ceux que l'expéditeur a chiffrés.

## Ce que le serveur voit / ne voit pas

| Visible côté serveur | Invisible |
|---|---|
| taille du ciphertext (≈ taille réelle) | contenu du fichier |
| hash du ciphertext | clé de déchiffrement |
| dates, fréquence d'accès | nom du fichier (sauf `--name` explicite) |

`--name` publie volontairement un nom en clair (affiché dans `/api/files`
et le `Content-Disposition`) — par défaut, rien.

## Limites et choix assumés

- **Perte du lien = perte du fichier.** Aucune récupération possible,
  c'est le contrat du zéro-connaissance.
- **Pas de dédup inter-fichiers** : deux uploads du même fichier donnent
  deux ciphertexts différents (clés différentes). Le chiffrement
  convergent la permettrait mais révèle l'égalité des contenus — refusé.
- La taille et les motifs d'accès restent observables (padding et
  couverture de trafic hors périmètre).
- `curl` peut toujours uploader du clair via l'API brute — le chiffrement
  est côté client par nature ; l'UI web l'appliquera systématiquement.

## Réquisition judiciaire : ce que l'opérateur peut fournir

| Peut fournir | Ne peut pas fournir |
|---|---|
| les ciphertexts (fichiers chiffrés reconstitués) | le contenu en clair |
| hashes, tailles, dates | les clés de déchiffrement |
| logs réseau si l'opérateur en conserve | — |
| suppression / blocage d'un hash (cf. backlog A) | — |

La clé n'a jamais transité vers le serveur (elle vit dans le fragment de
l'URL, que HTTP n'envoie pas) : il n'y a **rien à saisir** sur les nœuds
qui permette de déchiffrer. Les autorités récupèrent le contenu par le
**lien complet** — appareil de l'uploader ou d'un destinataire, messagerie
où il a circulé — et non auprès de l'hébergeur.

Corollaire pour l'opérateur : documenter ce dispositif, prévoir un point
de contact abus, et implémenter suppression + blocage par hash (backlog A)
**avant toute mise en ligne publique**. Les obligations de remise de clé
visent qui détient la clé — l'utilisateur, pas l'hébergeur. Ceci n'est pas
un avis juridique : faire valider par un avocat selon le pays et le statut
(hébergeur / éditeur).
