//! Vivaldi network coordinates: one position per node in a Euclidean space
//! where **distance predicts latency**.
//!
//! Every node measures the RTT to its peers (a QUIC ping is enough) and
//! adjusts its position: too far from a fast peer → it moves closer; too
//! close to a slow peer → it moves away. After a few dozen exchanges the
//! whole set converges to a consistent map — no GeoIP database, no
//! configuration, continuously self-calibrating.
//!
//! Two uses:
//! - **placement**: spread the shards of a single stripe (two close nodes
//!   are probably in the same datacenter, hence a correlated failure —
//!   separating them is how you survive the loss of a region);
//! - **reads**: prefer the closest peer.
//!
//! Reference: Dabek et al., *Vivaldi: A Decentralized Network Coordinate
//! System* (SIGCOMM'04), with the height modelling the last-mile access
//! cost.
//!
//! DETERMINISM: coordinates travel in the Raft state, so every node places
//! from the same values. Only basic IEEE operations are used here (the
//! square root is computed with Newton's method) — libm implementations
//! differ across platforms, and two nodes that ranked differently would
//! fight over shards.

use serde::{Deserialize, Serialize};

/// Dimensions of the Euclidean space (2 is enough for Earth geography;
/// the height captures the rest).
pub const DIMS: usize = 2;

/// Adjustment sensitivity: small = stable but slow to converge.
const CC: f64 = 0.25;
/// Sensitivity of the estimated error.
const CE: f64 = 0.25;
/// Minimum height (ms) — prevents collapse into a single point.
const MIN_HEIGHT: f64 = 1.0;
/// Initial error: "I know nothing about my position".
const MAX_ERROR: f64 = 1.5;

/// Position of a node in latency space.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Coord {
    /// Euclidean components, in milliseconds.
    pub vec: [f64; DIMS],
    /// Height: incompressible access latency (last mile).
    pub height: f64,
    /// Confidence in this position (0 = certain, 1.5 = unknown).
    pub error: f64,
}

impl Default for Coord {
    fn default() -> Self {
        Self { vec: [0.0; DIMS], height: MIN_HEIGHT, error: MAX_ERROR }
    }
}

/// Deterministic square root (Newton) — see the module-level note.
fn det_sqrt(x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    // Initial guess by exponent manipulation, then 6 iterations (well
    // converged in double precision over our ranges).
    let bits = x.to_bits();
    let mut guess = f64::from_bits((bits >> 1) + (0x1ff8_0000_0000_0000));
    for _ in 0..6 {
        guess = 0.5 * (guess + x / guess);
    }
    guess
}

impl Coord {
    /// Estimated distance (ms) between two positions: Euclidean + heights.
    pub fn distance(&self, other: &Coord) -> f64 {
        let mut sum = 0.0;
        for i in 0..DIMS {
            let d = self.vec[i] - other.vec[i];
            sum += d * d;
        }
        det_sqrt(sum) + self.height + other.height
    }

    /// Adjusts this position after measuring `rtt_ms` towards `peer`.
    ///
    /// The algorithm treats the network as a system of springs: every
    /// measurement pulls or pushes the node, with a step proportional to
    /// the relative confidence of the two positions.
    pub fn observe(&mut self, peer: &Coord, rtt_ms: f64) {
        if !(rtt_ms.is_finite()) || rtt_ms <= 0.0 {
            return;
        }
        let predicted = self.distance(peer);
        // Weight: if the peer is far more confident than we are, we move a
        // lot; if it is lost, we almost ignore it.
        let total_error = self.error + peer.error;
        let weight = if total_error > 0.0 { self.error / total_error } else { 0.5 };

        // Estimated error update (moving average).
        let relative_error = if rtt_ms > 0.0 {
            let e = predicted - rtt_ms;
            (if e < 0.0 { -e } else { e }) / rtt_ms
        } else {
            0.0
        };
        self.error = (relative_error * CE * weight + self.error * (1.0 - CE * weight))
            .clamp(0.0, MAX_ERROR);

        // Spring force: gap between measurement and prediction.
        let delta = CC * weight;
        let force = delta * (rtt_ms - predicted);

        // Unit direction from `peer` towards us.
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
            // Coincident positions: push along one axis to separate them.
            self.vec[0] += force;
        }

        // The height absorbs the latency geometry cannot explain.
        let height_delta = force * (self.height / (self.height + peer.height).max(1e-9));
        self.height = (self.height + height_delta).max(MIN_HEIGHT);
    }

    /// Is the position considered reliable? (used to ignore nodes that
    /// have not converged yet)
    pub fn is_settled(&self) -> bool {
        self.error < 0.5
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Simulates a real network and checks that the learned coordinates
    /// predict the latencies.
    fn converge(truth: &[(f64, f64)], rounds: usize) -> Vec<Coord> {
        let n = truth.len();
        let mut coords = vec![Coord::default(); n];
        // "Real" RTTs: Euclidean distance in the ground truth.
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
        // Three "cities": Paris, Frankfurt (close), Miami (far).
        let truth = [(0.0, 0.0), (10.0, 0.0), (90.0, 0.0)];
        let coords = converge(&truth, 400);

        let d_close = coords[0].distance(&coords[1]);
        let d_far = coords[0].distance(&coords[2]);
        assert!(
            d_far > d_close * 3.0,
            "the learned distance must reflect latency: close={d_close:.1} far={d_far:.1}"
        );
        // Positions must be deemed reliable after convergence.
        assert!(coords.iter().all(|c| c.is_settled()), "coordinates did not converge");
    }

    #[test]
    fn distance_is_symmetric_and_deterministic() {
        let a = Coord { vec: [3.0, 4.0], height: 2.0, error: 0.1 };
        let b = Coord { vec: [0.0, 0.0], height: 1.0, error: 0.1 };
        assert_eq!(a.distance(&b), b.distance(&a));
        // 5 (Euclidean) + 2 + 1 = 8
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
        assert_eq!(c, before, "invalid RTTs must change nothing");
    }
}
