use surrealdb::engine::local::SurrealKv;
use surrealdb::Surreal;

use crate::StateError;

const DEFAULT_PATH: &str = "/var/lib/nauka/db";
/// Namespace used by every Nauka instance. Exposed so higher layers
/// (IAM signin/signup, tests) can reference the same value the
/// embedded engine was opened with.
pub const DEFAULT_NAMESPACE: &str = "nauka";
pub const DEFAULT_DATABASE: &str = "nauka";

#[derive(Debug)]
pub struct Database {
    db: Surreal<surrealdb::engine::local::Db>,
}

impl Database {
    pub async fn open(path: Option<&str>) -> Result<Self, StateError> {
        let path = path.unwrap_or(DEFAULT_PATH);
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let db = Surreal::new::<SurrealKv>(path).await?;
        db.use_ns(DEFAULT_NAMESPACE)
            .use_db(DEFAULT_DATABASE)
            .await?;
        Ok(Self { db })
    }

    pub async fn query(&self, surql: &str) -> Result<(), StateError> {
        self.db.query(surql).await?.check()?;
        Ok(())
    }

    pub async fn query_take<T: surrealdb::types::SurrealValue>(
        &self,
        surql: &str,
    ) -> Result<Vec<T>, StateError> {
        let mut response = self.db.query(surql).await?;
        Ok(response.take(0)?)
    }

    pub fn inner(&self) -> &Surreal<surrealdb::engine::local::Db> {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn open_tmp() -> (Database, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(Some(path.to_str().unwrap())).await.unwrap();
        (db, dir)
    }

    #[tokio::test]
    async fn query_propagates_schema_violation() {
        let (db, _dir) = open_tmp().await;
        db.query(
            "DEFINE TABLE t SCHEMAFULL; \
             DEFINE FIELD name ON t TYPE string; \
             DEFINE INDEX t_name ON t FIELDS name UNIQUE;",
        )
        .await
        .unwrap();

        db.query("CREATE t SET name = 'foo'").await.unwrap();
        let result = db.query("CREATE t SET name = 'foo'").await;
        assert!(
            result.is_err(),
            "expected unique-constraint error, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn query_propagates_missing_required_field() {
        let (db, _dir) = open_tmp().await;
        db.query(
            "DEFINE TABLE t SCHEMAFULL; \
             DEFINE FIELD name ON t TYPE string;",
        )
        .await
        .unwrap();

        // Missing required `name` field
        let result = db.query("CREATE t SET other = 'x'").await;
        assert!(
            result.is_err(),
            "expected schema-violation error, got: {result:?}"
        );
    }
}
