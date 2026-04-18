use nauka_core::NaukaError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IamError {
    #[error("invalid email: {0}")]
    InvalidEmail(String),

    #[error("password: {0}")]
    Password(String),

    #[error("invalid credentials")]
    InvalidCredentials,

    #[error("user already exists: {0}")]
    UserExists(String),

    #[error("jwt: {0}")]
    Jwt(String),

    #[error("token file: {0}")]
    Token(String),

    #[error("state: {0}")]
    State(#[from] nauka_state::StateError),

    #[error("db: {0}")]
    Db(#[from] surrealdb::Error),
}

impl NaukaError for IamError {
    fn event_name(&self) -> &'static str {
        match self {
            IamError::InvalidEmail(_) => "iam.invalid_email",
            IamError::Password(_) => "iam.password",
            IamError::InvalidCredentials => "iam.invalid_credentials",
            IamError::UserExists(_) => "iam.user_exists",
            IamError::Jwt(_) => "iam.jwt",
            IamError::Token(_) => "iam.token",
            IamError::State(_) => "iam.state",
            IamError::Db(_) => "iam.db",
        }
    }
}
