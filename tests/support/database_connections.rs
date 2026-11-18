use nexora_runtime::DatabaseConnections;
use std::path::PathBuf;

pub fn configured_path() -> PathBuf {
    std::env::var_os("NEXORA_TEST_CONNECTIONS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../../.nexora/local/connections.json"))
}

pub fn load() -> DatabaseConnections {
    let configured_path = configured_path();

    DatabaseConnections::load(&configured_path).unwrap_or_else(|error| {
        panic!(
            "could not load integration-test database connections from {}: {error}",
            configured_path.display()
        )
    })
}
