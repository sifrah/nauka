# Correctif de sécurité Nauka

Ce répertoire reprend la source publiée de `reed-solomon-erasure` 6.0.0
(MIT, licence amont conservée). Le projet amont n'est plus maintenu.

Nauka ne change que deux détails d'implémentation :

- `lru` passe de 0.7.8 à 0.18.2, ce qui corrige
  RUSTSEC-2026-0253;
- la capacité fixe du cache devient le `NonZeroUsize` demandé par la
  nouvelle API de `lru`.

Aucun calcul de corps de Galois, encodage, reconstruction ou chemin
SIMD n'est modifié. Les suites complètes d'effacement et de cluster du
workspace exercent cette copie avant chaque release.
