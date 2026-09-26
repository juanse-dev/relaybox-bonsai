use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("internal")]
    Internal,

    #[error("repository error: {0}")]
    Repository(RepositoryError),
}

impl From<RepositoryError> for ApplicationError {
    fn from(err: RepositoryError) -> Self {
        Self::Repository(err)
    }
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("database error: {0}")]
    Db(String),
}
