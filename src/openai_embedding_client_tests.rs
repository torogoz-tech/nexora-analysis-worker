use super::OpenAiEmbeddingClient;
use serde_json::json;

#[test]
fn embedding_response_is_ordered_by_provider_index() {
    let decoded = OpenAiEmbeddingClient::decode_embeddings(
        &json!({"data":[
            {"index":1,"embedding":[2.0,3.0]},
            {"index":0,"embedding":[0.5,1.0]}
        ]}),
        2,
    )
    .unwrap();

    assert_eq!(decoded, vec![vec![0.5, 1.0], vec![2.0, 3.0]]);
}

#[test]
fn malformed_or_duplicate_embedding_indexes_are_rejected() {
    let duplicate = json!({"data":[
        {"index":0,"embedding":[1.0]},
        {"index":0,"embedding":[2.0]}
    ]});

    assert!(OpenAiEmbeddingClient::decode_embeddings(&duplicate, 2).is_err());
    assert!(OpenAiEmbeddingClient::decode_embeddings(&json!({"data":[]}), 1).is_err());
}

#[test]
fn non_finite_float32_embeddings_are_rejected() {
    let oversized = json!({"data":[{"index":0,"embedding":[3.5e38]}]});

    assert!(OpenAiEmbeddingClient::decode_embeddings(&oversized, 1).is_err());
}
