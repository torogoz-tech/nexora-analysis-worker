use lapin::{message::Delivery, options::BasicAckOptions, options::BasicNackOptions};
use nexora_platform::PlatformError;

pub struct RabbitMqJobWakeupDelivery {
    delivery: Delivery,
}

impl RabbitMqJobWakeupDelivery {
    pub(crate) fn new(delivery: Delivery) -> Self {
        Self { delivery }
    }

    pub async fn ack(self) -> Result<(), PlatformError> {
        self.delivery
            .ack(BasicAckOptions::default())
            .await
            .map_err(|_| PlatformError::Unavailable)
            .map(|_| ())
    }

    pub async fn retry(self) -> Result<(), PlatformError> {
        self.delivery
            .nack(BasicNackOptions {
                requeue: true,
                ..BasicNackOptions::default()
            })
            .await
            .map_err(|_| PlatformError::Unavailable)
            .map(|_| ())
    }
}
