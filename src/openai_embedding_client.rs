use nexora_platform::{OpenAiEndpoint, PlatformError};
use serde_json::{Value, json};
use std::time::Duration;
use zeroize::Zeroizing;

pub(crate) struct OpenAiEmbeddingClient {
    agent: ureq::Agent,
    key: Zeroizing<String>,
    endpoint: String,
}

impl OpenAiEmbeddingClient {
    pub(crate) fn new(
        key: Zeroizing<String>,
        model: String,
        endpoint: &str,
    ) -> Result<Self, PlatformError> {
        Self::validate_model(&model)?;
        if key.is_empty() || key.len() > 4096 || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(PlatformError::Validation("invalid OpenAI credential"));
        }
        let endpoint = OpenAiEndpoint::normalize(endpoint)?;
        Ok(Self {
            agent: ureq::Agent::config_builder()
                .proxy(None)
                .max_redirects(0)
                .http_status_as_error(false)
                .timeout_global(Some(Duration::from_secs(90)))
                .build()
                .into(),
            key,
            endpoint,
        })
    }

    pub(crate) fn embed_many(
        &self,
        model: &str,
        inputs: &[String],
    ) -> Result<Vec<Vec<f32>>, PlatformError> {
        Self::validate_model(model)?;
        Self::validate_inputs(inputs)?;
        let endpoint = self
            .endpoint
            .strip_suffix("/responses")
            .map(|base| format!("{base}/embeddings"))
            .ok_or(PlatformError::Validation("invalid OpenAI endpoint"))?;
        let body = json!({
            "model": model,
            "input": inputs,
            "encoding_format": "float"
        });
        let encoded = serde_json::to_vec(&body).map_err(|_| PlatformError::Unavailable)?;
        if encoded.len() > 8 * 1024 * 1024 {
            return Err(PlatformError::Validation("embedding request exceeds limit"));
        }
        let bearer = Zeroizing::new(format!("Bearer {}", self.key.as_str()));
        let mut authorization = ureq::http::HeaderValue::from_str(&bearer)
            .map_err(|_| PlatformError::Validation("invalid OpenAI credential"))?;
        authorization.set_sensitive(true);
        let mut response = self
            .agent
            .post(endpoint)
            .config()
            .timeout_global(Some(Duration::from_secs(90)))
            .build()
            .header("Authorization", authorization)
            .header("Content-Type", "application/json")
            .send(encoded.as_slice())
            .map_err(|_| PlatformError::Unavailable)?;
        if response.status().as_u16() != 200 {
            return Err(PlatformError::Unavailable);
        }
        let bytes = response
            .body_mut()
            .with_config()
            .limit(1_048_576)
            .read_to_vec()
            .map_err(|_| PlatformError::Unavailable)?;
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| PlatformError::Unavailable)?;
        Self::decode_embeddings(&value, inputs.len())
    }

    fn validate_model(model: &str) -> Result<(), PlatformError> {
        if model.is_empty()
            || model.len() > 128
            || !model
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-._:/".contains(&byte))
        {
            return Err(PlatformError::Validation("invalid embedding model"));
        }
        Ok(())
    }

    fn validate_inputs(inputs: &[String]) -> Result<(), PlatformError> {
        if inputs.is_empty()
            || inputs.len() > 64
            || inputs
                .iter()
                .any(|input| input.is_empty() || input.len() > 32_000)
            || inputs.iter().map(String::len).sum::<usize>() > 2_000_000
        {
            return Err(PlatformError::Validation("invalid embedding inputs"));
        }
        Ok(())
    }

    fn decode_embeddings(
        value: &Value,
        input_count: usize,
    ) -> Result<Vec<Vec<f32>>, PlatformError> {
        let items = value["data"].as_array().ok_or(PlatformError::Unavailable)?;
        if items.len() != input_count {
            return Err(PlatformError::Unavailable);
        }
        let mut embeddings = vec![None; input_count];
        for item in items {
            let index = item["index"]
                .as_u64()
                .and_then(|index| usize::try_from(index).ok())
                .filter(|index| *index < input_count)
                .ok_or(PlatformError::Unavailable)?;
            if embeddings[index].is_some() {
                return Err(PlatformError::Unavailable);
            }
            let embedding = item["embedding"]
                .as_array()
                .ok_or(PlatformError::Unavailable)?
                .iter()
                .map(|value| {
                    let number = value
                        .as_f64()
                        .filter(|number| number.is_finite())
                        .ok_or(PlatformError::Unavailable)? as f32;
                    number
                        .is_finite()
                        .then_some(number)
                        .ok_or(PlatformError::Unavailable)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if embedding.is_empty() {
                return Err(PlatformError::Unavailable);
            }
            embeddings[index] = Some(embedding);
        }
        embeddings
            .into_iter()
            .map(|embedding| embedding.ok_or(PlatformError::Unavailable))
            .collect()
    }
}

#[cfg(test)]
#[path = "openai_embedding_client_tests.rs"]
mod tests;
