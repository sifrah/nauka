use surrealdb::engine::local::SurrealKv;
use surrealdb::Surreal;

use crate::StateError;

const DEFAULT_PATH: &str = "/var/lib/nauka/db";
const NS: &str = "nauka";
const DB: &str = "nauka";

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
        db.use_ns(NS).use_db(DB).await?;
        Ok(Self { db })
    }

    pub async fn query(&self, surql: &str) -> Result<(), StateError> {
        self.db.query(surql).await?;
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
