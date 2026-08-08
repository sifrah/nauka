//! S3-compatible API for Nauka.
//!
//! The target is the full S3 surface as AWS defines it — 99 operations in
//! the generated service trait — with self-hosted semantics wherever AWS
//! wires in a proprietary service. Conformance is not asserted, it is
//! measured: the `ceph/s3-tests` suite runs in CI and every excluded test
//! carries its reason.
//!
//! This crate holds the storage-facing half: the data model replicated by
//! Raft ([`model`]) and the naming rules S3 imposes ([`naming`]). The HTTP
//! half plugs it into the `s3s` service trait.

pub mod creds;
pub mod model;
pub mod naming;
pub mod policy;

pub use creds::{generate_credential, Action};
pub use model::{
    new_version_id, Bucket, BucketPermission, Credential, MultipartUpload, ObjectEntry,
    ObjectVersion, S3State, UploadedPart, VersioningState,
};
pub use policy::{Decision, Policy, PolicyError, Requester};
