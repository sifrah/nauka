//! Coordonnées réseau Vivaldi : une position par nœud dans un espace
//! euclidien où **la distance prédit la latence**.
//!
//! Chaque nœud mesure le RTT vers ses pairs (un ping QUIC suffit) et
//! ajuste sa position : trop loin d'un pair rapide → il s'en rapproche ;
//! trop près d'un pair lent → il s'en éloigne. Après quelques dizaines
//! d'échanges, l'ensemble converge vers une carte cohérente — sans base
//! GeoIP, sans configuration, auto-calibrée en continu.
//!
//! Deux usages :
//! - **placement** : écarter les shards d'une même stripe (deux nœuds
//!   proches sont probablement dans le même datacenter, donc corrélés en
//!   panne — les séparer, c'est survivre à la perte d'une région) ;
//! - **lecture** : préférer le pair le plus proche.
//!
//! Référence : Dabek et al., *Vivaldi: A Decentralized Network Coordinate
//! System* (SIGCOMM'04), avec la hauteur (« height ») qui modélise le coût
//! d'accès du dernier kilomètre.
//!
//! DÉTERMINISME : les coordonnées circulent dans l'état Raft, donc tous
//! les nœuds placent à partir des mêmes valeurs. Seules des opérations
//! IEEE de base sont utilisées ici (la racine carrée est calculée par
//! Newton) — les libm diffèrent d'une plateforme à l'autre, et deux nœuds
//! qui classeraient différemment se disputeraient les shards.

use serde::{Deserialize, Serialize};

/// Dimensions de l'espace euclidien (2 suffit pour la géographie
/// terrestre ; la hauteur capture le reste).
pub const DIMS: usize = 2;

/// Sensibilité de l'ajustement : petite = stable mais lent à converger.
const CC: f64 = 0.25;
/// Sensibilité de l'erreur estimée.
const CE: f64 = 0.25;
/// Hauteur minimale (ms) — évite l'effondrement en un point.
const MIN_HEIGHT: f64 = 1.0;
/// Erreur initiale : « je ne sais rien de ma position ».
const MAX_ERROR: f64 = 1.5;

/// Position d'un nœud dans l'espace latence.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Coord {
    /// Composantes euclidiennes, en millisecondes.
    pub vec: [f64; DIMS],
    /// Hauteur : latence d'accès incompressible (dernier kilomètre).
    pub height: f64,
    /// Confiance dans cette position (0 = sûre, 1.5 = inconnue).
    pub error: f64,
}

impl Default for Coord {
    fn default() -> Self {
        Self { vec: [0.0; DIMS], height: MIN_HEIGHT, error: MAX_ERROR }
    }
}

/// Racine carrée déterministe (Newton) — voir la note en tête de module.
fn det_sqrt(x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    // Estimation initiale par manipulation d'exposant, puis 6 itérations
    // (largement convergé en double précision pour nos plages).
    let bits = x.to_bits();
    let mut guess = f64::from_bits((bits >> 1) + (0x1ff8_0000_0000_0000));
    for _ in 0..6 {
        guess = 0.5 * (guess + x / guess);
    }
    guess
}

impl Coord {
    /// Distance estimée (ms) entre deux positions : euclidienne + hauteurs.
    pub fn distance(&self, other: &Coord) -> f64 {
        let mut sum = 0.0;
        for i in 0..DIMS {
            let d = self.vec[i] - other.vec[i];
            sum += d * d;
        }
        det_sqrt(sum) + self.height + other.height
    }

    /// Ajuste cette position après avoir mesuré `rtt_ms` vers `peer`.
    ///
    /// L'algorithme traite le réseau comme un système de ressorts : chaque
    /// mesure tire ou repousse le nœud, avec un pas proportionnel à la
    /// confiance relative des deux positions.
    pub fn observe(&mut self, peer: &Coord, rtt_ms: f64) {
        if !(rtt_ms.is_finite()) || rtt_ms <= 0.0 {
            return;
        }
        let predicted = self.distance(peer);
        // Poids : si le pair est bien plus sûr de lui que nous, on bouge
        // beaucoup ; s'il est perdu, on l'ignore presque.
        let total_error = self.error + peer.error;
        let weight = if total_error > 0.0 { self.error / total_error } else { 0.5 };

        // Mise à jour de l'erreur estimée (moyenne mobile).
        let relative_error = if rtt_ms > 0.0 {
            let e = predicted - rtt_ms;
            (if e < 0.0 { -e } else { e }) / rtt_ms
        } else {
            0.0
        };
        self.error = (relative_error * CE * weight + self.error * (1.0 - CE * weight))
            .clamp(0.0, MAX_ERROR);

        // Force du ressort : écart entre mesure et prédiction.
        let delta = CC * weight;
        let force = delta * (rtt_ms - predicted);

        // Direction unitaire de `peer` vers nous.
        let mut dir = [0.0; DIMS];
        let mut norm_sq = 0.0;
        for i in 0..DIMS {
            dir[i] = self.vec[i] - peer.vec[i];
            norm_sq += dir[i] * dir[i];
        }
        let norm = det_sqrt(norm_sq);
        if norm > 1e-9 {
            for i in 0..DIMS {
                self.vec[i] += force * (dir[i] / norm);
            }
        } else {
            // Positions confondues : pousse sur un axe pour les séparer.
            self.vec[0] += force;
        }

        // La hauteur absorbe la latence non explicable par la géométrie.
        let height_delta = force * (self.height / (self.height + peer.height).max(1e-9));
        self.height = (self.height + height_delta).max(MIN_HEIGHT);
    }

    /// Position jugée fiable ? (sert à ignorer les nœuds non convergés)
    pub fn is_settled(&self) -> bool {
        self.error < 0.5
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simule un réseau réel et vérifie que les coordonnées apprises
    /// prédisent les latences.
    fn converge(truth: &[(f64, f64)], rounds: usize) -> Vec<Coord> {
        let n = truth.len();
        let mut coords = vec![Coord::default(); n];
        // RTT « réels » : distance euclidienne dans la vérité terrain.
        let real_rtt = |a: usize, b: usize| -> f64 {
            let dx = truth[a].0 - truth[b].0;
            let dy = truth[a].1 - truth[b].1;
            (dx * dx + dy * dy).sqrt() + 2.0
        };
        for r in 0..rounds {
            for a in 0..n {
                let b = (a + 1 + r % (n - 1)) % n;
                let peer = coords[b];
                coords[a].observe(&peer, real_rtt(a, b));
            }
        }
        coords
    }

    #[test]
    fn coordinates_predict_latency() {
        // Trois « villes » : Paris, Francfort (proche), Miami (loin).
        let truth = [(0.0, 0.0), (10.0, 0.0), (90.0, 0.0)];
        let coords = converge(&truth, 400);

        let d_close = coords[0].distance(&coords[1]);
        let d_far = coords[0].distance(&coords[2]);
        assert!(
            d_far > d_close * 3.0,
            "la distance apprise doit refléter la latence: proche={d_close:.1} loin={d_far:.1}"
        );
        // Les positions doivent être jugées fiables après convergence.
        assert!(coords.iter().all(|c| c.is_settled()), "coordonnées non convergées");
    }

    #[test]
    fn distance_is_symmetric_and_deterministic() {
        let a = Coord { vec: [3.0, 4.0], height: 2.0, error: 0.1 };
        let b = Coord { vec: [0.0, 0.0], height: 1.0, error: 0.1 };
        assert_eq!(a.distance(&b), b.distance(&a));
        // 5 (euclidien) + 2 + 1 = 8
        assert!((a.distance(&b) - 8.0).abs() < 1e-9, "{}", a.distance(&b));
    }

    #[test]
    fn det_sqrt_matches_libm() {
        for x in [0.0, 1e-9, 1.0, 2.0, 9.0, 1234.5678, 1e6, 1e12] {
            let got = det_sqrt(x);
            let want = x.sqrt();
            assert!((got - want).abs() <= want.abs() * 1e-12 + 1e-12, "sqrt({x}): {got} vs {want}");
        }
    }

    #[test]
    fn absurd_measurements_are_ignored() {
        let mut c = Coord::default();
        let before = c;
        c.observe(&Coord::default(), -5.0);
        c.observe(&Coord::default(), f64::NAN);
        assert_eq!(c, before, "les RTT invalides ne doivent rien changer");
    }
}
