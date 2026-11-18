use crate::{RabbitMqJobWakeupDelivery, RabbitMqJobWakeupTopology};
use futures_lite::StreamExt;
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicConsumeOptions, BasicQosOptions},
    types::FieldTable,
};
use nexora_platform::{JobKind, JobWakeup, PlatformError};

pub struct RabbitMqJobWakeupConsumer {
    _connection: Connection,
    consumer: lapin::Consumer,
    expected_kind: JobKind,
}

impl RabbitMqJobWakeupConsumer {
    pub async fn connect(uri: &str, expected_kind: JobKind) -> Result<Self, PlatformError> {
        let connection = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        let channel = connection
            .create_channel()
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        RabbitMqJobWakeupTopology::declare(&channel).await?;
        channel
            .basic_qos(1, BasicQosOptions::default())
            .await
            .map_err(|_| PlatformError::Unavailable)?;
        let consumer = channel
            .basic_consume(
                expected_kind.queue_name().into(),
                "".into(),
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|_| PlatformError::Unavailable)?;

        Ok(Self {
            _connection: connection,
            consumer,
            expected_kind,
        })
    }

    pub async fn next(&mut self) -> Result<Option<RabbitMqJobWakeupDelivery>, PlatformError> {
        loop {
            let Some(delivery) = self.consumer.next().await else {
                return Ok(None);
            };
            let delivery = delivery.map_err(|_| PlatformError::Unavailable)?;
            let message_id = delivery
                .properties
                .message_id()
                .as_ref()
                .map(|message_id| message_id.as_str());
            let valid = Self::is_expected_wakeup(&delivery.data, message_id, self.expected_kind);
            if !valid {
                delivery
                    .reject(lapin::options::BasicRejectOptions { requeue: false })
                    .await
                    .map_err(|_| PlatformError::Unavailable)?;
                continue;
            }

            return Ok(Some(RabbitMqJobWakeupDelivery::new(delivery)));
        }
    }

    fn is_expected_wakeup(body: &[u8], message_id: Option<&str>, expected_kind: JobKind) -> bool {
        serde_json::from_slice::<JobWakeup>(body).is_ok_and(|wakeup| {
            wakeup.kind().queue_name() == expected_kind.queue_name()
                && message_id == Some(wakeup.job_id())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RabbitMqJobWakeupConsumer;
    use nexora_platform::{JobKind, JobWakeup};

    #[test]
    fn accepts_only_matching_queue_wakeups() {
        for kind in [JobKind::RepositorySync, JobKind::RepositoryIndex] {
            let wakeup = JobWakeup::new("repo_job_1".into(), kind).unwrap();
            let body = serde_json::to_vec(&wakeup).unwrap();

            assert!(RabbitMqJobWakeupConsumer::is_expected_wakeup(
                &body,
                Some("repo_job_1"),
                JobKind::RepositoryIndex
            ));
            assert!(!RabbitMqJobWakeupConsumer::is_expected_wakeup(
                &body,
                Some("other_job"),
                JobKind::RepositoryIndex
            ));
        }

        let release = JobWakeup::new("release_1".into(), JobKind::ReleaseComposition).unwrap();
        let body = serde_json::to_vec(&release).unwrap();
        assert!(!RabbitMqJobWakeupConsumer::is_expected_wakeup(
            &body,
            Some("release_1"),
            JobKind::RepositoryIndex
        ));
        assert!(!RabbitMqJobWakeupConsumer::is_expected_wakeup(
            b"not-json",
            Some("repo_job_1"),
            JobKind::RepositoryIndex
        ));
    }
}
