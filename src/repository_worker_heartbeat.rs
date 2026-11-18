use nexora_analysis_store::{AnalysisStore, RepositoryJobLease};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError},
    },
    time::Duration,
};

pub(crate) struct RepositoryWorkerHeartbeat;
impl RepositoryWorkerHeartbeat {
    pub fn run(
        mut store: AnalysisStore,
        lease: &RepositoryJobLease,
        stop: Receiver<()>,
        lost: &AtomicBool,
    ) {
        loop {
            match stop.recv_timeout(Duration::from_secs(30)) {
                Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => {
                    if store.renew_repository_job(lease, 300).is_err() {
                        lost.store(true, Ordering::Release);
                        return;
                    }
                }
            }
        }
    }
}
