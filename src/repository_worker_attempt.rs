use crate::RepositoryWorkerOutcome;

pub(crate) enum RepositoryWorkerAttempt {
    NoWork,
    Completed(RepositoryWorkerOutcome),
    StaleLease,
}
