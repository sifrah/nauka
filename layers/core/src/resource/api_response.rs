//! Enforced API response format for cloud resources.
//!
//! Every resource that goes through the API must implement [`ApiResource`].
//! The trait guarantees system fields (id, name, status, labels, timestamps)
//! are always present and correctly formatted.
//!
//! ```
//! use nauka_core::resource::{ApiResource, ResourceMeta};
//!
//! struct Vpc {
//!     meta: ResourceMeta,
//!     cidr: String,
//! }
//!
//! impl ApiResource for Vpc {
//!     fn meta(&self) -> &ResourceMeta { &self.meta }
//!     fn resource_fields(&self) -> serde_json::Value {
//!         serde_json::json!({"cidr": self.cidr})
//!     }
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// System fields present on every cloud resource.
///
/// Embed in your resource struct with `#[serde(flatten)]`.
/// Use `ResourceMeta::new()` to create with defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMeta {
    /// Unique resource ID (e.g., "org-01KNQ0YKTG...")
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Resource lifecycle status (active, deleting, etc.)
    pub status: String,
    /// User-defined key-value labels
    pub labels: HashMap<String, String>,
    /// Creation timestamp (Unix epoch seconds)
    pub created_at: u64,
    /// Last update timestamp (Unix epoch seconds)
    pub updated_at: u64,
}

impl ResourceMeta {
    /// Create a new ResourceMeta with sensible defaults.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            id: id.into(),
            name: name.into(),
            status: "active".to_string(),
            labels: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }
}

/// Trait that every API-facing resource must implement.
///
/// Guarantees consistent JSON output with system fields + ISO 8601 timestamps.
/// Implement `resource_fields()` to add resource-specific fields.
pub trait ApiResource {
    /// Return the system metadata.
    fn meta(&self) -> &ResourceMeta;

    /// Return resource-specific fields as a JSON object.
    /// These are merged into the response alongside system fields.
    fn resource_fields(&self) -> serde_json::Value {
        serde_json::json!({})
    }

    /// Serialize to the API response format.
    ///
    /// System fields are always present and timestamps are ISO 8601.
    /// Resource-specific fields from `resource_fields()` are merged in.
    fn to_api_json(&self) -> serde_json::Value {
        let meta = self.meta();
        let mut obj = serde_json::json!({
            "id": meta.id,
            "name": meta.name,
            "status": meta.status,
            "labels": meta.labels,
            "created_at": epoch_to_iso8601(meta.created_at),
            "updated_at": epoch_to_iso8601(meta.updated_at),
        });

        // Merge resource-specific fields
        if let serde_json::Value::Object(extra) = self.resource_fields() {
            if let serde_json::Value::Object(ref mut map) = obj {
                map.extend(extra);
            }
        }

        obj
    }
}

/// Convert epoch seconds to ISO 8601 UTC string.
pub fn epoch_to_iso8601(epoch_secs: u64) -> String {
    let days = epoch_secs / 86400;
    let remaining = epoch_secs % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    let (year, month, day) = days_to_date(days);
    format!("{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}Z")
}

fn days_to_date(days: u64) -> (u64, u64, u64) {
    let mut y = 1970u64;
    let mut remaining = days;
    loop {
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let diy = if leap { 366 } else { 365 };
        if remaining < diy {
            break;
        }
        remaining -= diy;
        y += 1;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let months: [u64; 12] = if leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 0;
    for (i, &dim) in months.iter().enumerate() {
        if remaining < dim {
            m = i as u64 + 1;
            break;
        }
        remaining -= dim;
    }
    if m == 0 {
        m = 12;
    }
    (y, m, remaining + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestVpc {
        meta: ResourceMeta,
        cidr: String,
    }

    impl ApiResource for TestVpc {
        fn meta(&self) -> &ResourceMeta {
            &self.meta
        }
        fn resource_fields(&self) -> serde_json::Value {
            serde_json::json!({"cidr": self.cidr})
        }
    }

    #[test]
    fn meta_new_defaults() {
        let m = ResourceMeta::new("vpc-123", "web");
        assert_eq!(m.id, "vpc-123");
        assert_eq!(m.name, "web");
        assert_eq!(m.status, "active");
        assert!(m.labels.is_empty());
        assert!(m.created_at > 0);
        assert_eq!(m.created_at, m.updated_at);
    }

    #[test]
    fn to_api_json_has_system_fields() {
        let vpc = TestVpc {
            meta: ResourceMeta::new("vpc-01", "web"),
            cidr: "10.0.0.0/16".to_string(),
        };
        let json = vpc.to_api_json();
        assert_eq!(json["id"], "vpc-01");
        assert_eq!(json["name"], "web");
        assert_eq!(json["status"], "active");
        assert_eq!(json["cidr"], "10.0.0.0/16");
        // Timestamps are ISO 8601 strings
        let ts = json["created_at"].as_str().unwrap();
        assert!(ts.ends_with('Z'));
        assert!(ts.contains('T'));
    }

    #[test]
    fn to_api_json_merges_fields() {
        let vpc = TestVpc {
            meta: ResourceMeta::new("vpc-01", "web"),
            cidr: "10.0.0.0/16".to_string(),
        };
        let json = vpc.to_api_json();
        // System fields
        assert!(json["id"].is_string());
        assert!(json["name"].is_string());
        assert!(json["status"].is_string());
        assert!(json["labels"].is_object());
        assert!(json["created_at"].is_string());
        assert!(json["updated_at"].is_string());
        // Resource field
        assert!(json["cidr"].is_string());
    }

    #[test]
    fn epoch_to_iso8601_known() {
        assert_eq!(epoch_to_iso8601(0), "1970-01-01T00:00:00Z");
        assert_eq!(epoch_to_iso8601(1775665838), "2026-04-08T16:30:38Z");
    }

    #[test]
    fn no_extra_fields() {
        struct Bare {
            meta: ResourceMeta,
        }
        impl ApiResource for Bare {
            fn meta(&self) -> &ResourceMeta {
                &self.meta
            }
        }
        let b = Bare {
            meta: ResourceMeta::new("x-01", "bare"),
        };
        let json = b.to_api_json();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 6); // id, name, status, labels, created_at, updated_at
    }
}
