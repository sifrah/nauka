use crate::db::Database;
use crate::StateError;

pub async fn load_schemas(db: &Database, definitions: &[&str]) -> Result<(), StateError> {
    for surql in definitions {
        db.query(surql).await?;
    }
    Ok(())
}
