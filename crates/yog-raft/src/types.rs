//! Types du consensus : commandes appliquées à la state machine répliquée.

use std::collections::BTreeMap;
use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use yog_erasure::FileManifest;

pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Configuration openraft de yogfile.
    pub TypeConfig:
        D = AppCommand,
        R = AppResponse,
        NodeId = NodeId,
        Node = BasicNode,
);

/// Commandes répliquées par Raft. Les octets des shards ne passent JAMAIS
/// par le log de consensus — seules les métadonnées y transitent ; les
/// shards voyagent en direct par le transport QUIC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppCommand {
    /// Enregistre un fichier dans le registre répliqué du cluster.
    RegisterManifest(FileManifest),
    /// Retire un fichier du registre (les shards seront purgés par le GC).
    UnregisterManifest { file_hash: String },
    /// Déclare la capacité disque d'un nœud (poids du placement pondéré).
    /// Keyé par adresse annoncée — la même identité que le placement.
    UpdateNodeStats { addr: String, capacity_bytes: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppResponse {
    pub ok: bool,
    pub info: Option<String>,
}

/// État matérialisé par la state machine : le registre des fichiers et
/// les capacités déclarées des nœuds.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppState {
    pub manifests: BTreeMap<String, FileManifest>,
    /// Capacité disque déclarée par nœud (adresse → octets). Sert de poids
    /// au placement pondéré ; absent = capacité par défaut.
    #[serde(default)]
    pub node_capacities: BTreeMap<String, u64>,
}

/// Requêtes d'administration adressées à un nœud (hors log Raft).
#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRequest {
    /// Initialise le cluster avec ces membres (une seule fois, sur un nœud).
    Init(BTreeMap<NodeId, String>),
    /// Ajoute un nœud comme learner (rattrape le log sans voter).
    AddLearner { id: NodeId, addr: String },
    /// Change l'ensemble des membres votants.
    ChangeMembership(Vec<NodeId>),
    /// Écrit une commande via le leader (redirigée si besoin).
    Write(AppCommand),
    /// Vue du cluster : leader, membres, état du log.
    Metrics,
    /// Liste des manifests du registre répliqué.
    ListManifests,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminResponse {
    Ok(AppResponse),
    /// Ce nœud n'est pas leader ; réessayer sur `leader`.
    ForwardTo { leader: Option<(NodeId, String)> },
    Metrics {
        id: NodeId,
        leader: Option<NodeId>,
        members: BTreeMap<NodeId, String>,
        last_applied: Option<u64>,
        /// Capacités déclarées (adresse → octets) — la vue pondérée du
        /// placement, pour que les clients placent comme le cluster.
        #[serde(default)]
        capacities: BTreeMap<String, u64>,
    },
    Manifests(Vec<String>),
    Err(String),
}
