#![cfg(any(target_os = "linux", target_os = "macos"))]
#[path = "support/database_connections.rs"]
mod database_connections;
#[path = "support/git_fixture.rs"]
mod git_fixture;
#[path = "support/gitlab_source_fixture.rs"]
mod gitlab_source_fixture;
#[path = "support/test_directory.rs"]
mod test_directory;
use git_fixture::GitFixture;
use gitlab_source_fixture::GitlabSourceFixture;
use nexora_analysis_store::GitNetworkPolicy;
use nexora_analysis_worker::RepositoryWorker;
use nexora_core::{RepositoryId, SnapshotId, TenantId};
use nexora_platform::{
    GitProvider, GitlabClientRegistration, RepositoryJobKind, RepositoryJobRequest,
    RepositoryJobState, RepositoryRegistration,
};
use nexora_platform_store::{PlatformStore, ProviderSecretContext, SecretGenerator};
use nexora_runtime::DatabaseRuntime;
use serde_json::json;
use std::{
    os::unix::fs::DirBuilderExt,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

#[test]
#[ignore = "requires local databases; provider proof through encrypted credential, Git fetch, exact source and scoped publication"]
fn worker_publishes_one_mixed_snapshot_with_frozen_candidates_and_safe_mcp_provenance() {
    let git = GitFixture::new("job-source-plan");
    let code = b"export function generated() { return 7; }\n";
    let blob = git.blob(code);
    let lua = git.blob(b"function mixed() return 1 end\n");
    let unknown = git.blob(b"uncovered\n");
    let attributes = git.blob(b"source.js export-ignore\n");
    let tree = git.tree(&[
        ("100644", &attributes, ".gitattributes"),
        ("100644", &blob, "source.js"),
        ("100644", &lua, "source.lua"),
        ("100644", &unknown, "notes.unknown"),
    ]);
    let commit = git.commit(&tree);
    git.run(&["update-ref", "refs/heads/main", &commit], b"");
    git.run(&["symbolic-ref", "HEAD", "refs/heads/main"], b"");
    let provider = GitlabSourceFixture::start(&git, &commit);
    provider.control(json!({}));
    let config = database_connections::load();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let namespace = format!("nexora_platform_worker_{nonce}");
    let password = Zeroizing::new(SecretGenerator::token("fixture_password").unwrap());
    let mut store = PlatformStore::connect(&config, &namespace).unwrap();
    store.initialize().unwrap();
    store
        .bootstrap("Source plan fixture", "owner", &password, "Owner")
        .unwrap();
    let session = store.login("owner", &password).unwrap();
    let verified = store
        .identity_gate(&session.access_token, "skip", "")
        .unwrap();
    let ctx = store
        .authenticate(verified["access_token"].as_str().unwrap())
        .unwrap();
    let policy = GitNetworkPolicy {
        gitlab_origin: provider.origin().into(),
        ..GitNetworkPolicy::default()
    };
    let api_policy: nexora_platform_store::GitNetworkPolicy =
        serde_json::from_value(serde_json::to_value(&policy).unwrap()).unwrap();
    let client_secret = Zeroizing::new(SecretGenerator::token("fixture_client_secret").unwrap());
    store
        .configure_gitlab(
            &ctx,
            &GitlabClientRegistration {
                client_id: "fixture-client".into(),
                client_secret: client_secret.to_string(),
                redirect_uri: format!("{}/api/v1/git/gitlab/callback", policy.callback_origin),
                enabled: true,
                expected_revision: 0,
            },
            &api_policy,
        )
        .unwrap();
    let project = store.create_project(&ctx, "Source", None).unwrap();
    let repository = store
        .register_repository(
            &ctx,
            &project.id,
            &RepositoryRegistration {
                provider: GitProvider::Gitlab,
                full_name: "fixture/source".into(),
            },
        )
        .unwrap();
    let connection = SecretGenerator::id("conn").unwrap();
    let host = policy.host().unwrap();
    let tokens = Zeroizing::new(
        serde_json::to_vec(&json!({
            "access_token":provider.token(),"refresh_token":null,"token_type":"Bearer",
            "scope":"read_api read_repository","expires_in":3600
        }))
        .unwrap(),
    );
    let encrypted = serde_json::to_value(
        store
            .provider_secret_cipher()
            .unwrap()
            .seal(
                &ProviderSecretContext {
                    binding_id: connection.clone(),
                    provider: GitProvider::Gitlab.into(),
                    host: host.clone(),
                    purpose: "gitlab_tokens".into(),
                    revision: 1,
                },
                &tokens,
            )
            .unwrap(),
    )
    .unwrap();
    let pg_password = Zeroizing::new(
        std::fs::read_to_string(config.secrets_directory.join("postgres_password")).unwrap(),
    );
    let mut sql = postgres::Config::new()
        .host(&config.host)
        .port(config.postgres_port)
        .user(&config.postgres_user)
        .password(pg_password.trim())
        .dbname(&config.postgres_database)
        .connect(postgres::NoTls)
        .unwrap();
    sql.batch_execute(&format!("SET search_path TO \"{namespace}\""))
        .unwrap();
    sql.execute("INSERT INTO git_connections(id,provider,host,owner_actor_id,remote_account_id,remote_login,status,config_revision,tokens,expires_at) VALUES($1,'gitlab',$2,$3,'42','fixture','active',1,$4,now()+interval '1 hour')", &[&connection,&host,&ctx.actor_id(),&encrypted]).unwrap();
    sql.execute("UPDATE project_repositories SET host=$3,remote_id='101',remote_full_name='fixture/source',connection_id=$1,authorization_status='authorized',revision=2 WHERE id=$2", &[&connection,&repository.id,&host]).unwrap();
    let mut imported = Vec::new();
    for language in ["javascript", "lua"] {
        let package = nexora_language::AnalysisProfilePackage::from_catalog(
            &Path::new("tests/fixtures/languages").join(language),
            language.into(),
            format!("mixed-{language}"),
            "1.9.0".into(),
            format!("Mixed {language}"),
        )
        .unwrap();
        imported.push(store.import_analysis_profile(&ctx, &package).unwrap());
    }
    let input = RepositoryJobRequest {
        expected_revision: 2,
    };
    let assignment = store
        .repository_analysis_profile(&ctx, &project.id, &repository.id)
        .unwrap();
    assert_eq!(assignment["mode"], "automatic");
    let job = store
        .enqueue_repository_job(
            &ctx,
            &project.id,
            &repository.id,
            RepositoryJobKind::Index,
            &input,
        )
        .unwrap();
    // Importing a newer model cannot change a queued job's candidate set.
    let newer = nexora_language::AnalysisProfilePackage::from_catalog(
        Path::new("tests/fixtures/languages/javascript"),
        "javascript".into(),
        "mixed-javascript".into(),
        "1.10.0".into(),
        "New JavaScript".into(),
    )
    .unwrap();
    store.import_analysis_profile(&ctx, &newer).unwrap();
    let managed = git.directory.path().canonicalize().unwrap().join("managed");
    let mut directory = std::fs::DirBuilder::new();
    directory.mode(0o700).create(&managed).unwrap();
    let settings = serde_json::from_value(json!({
        "id":"mixed-profile-worker", "source_root":managed,
        "git_binary":"/usr/bin/git", "profiles":{}
    }))
    .unwrap();
    let mut worker = RepositoryWorker::connect(&config, &namespace, settings, policy).unwrap();
    let result = worker.run_once().unwrap().unwrap();
    assert_eq!(result.id, job.id);
    assert_eq!(
        result.state,
        RepositoryJobState::Succeeded,
        "{:?}",
        result.error_code
    );
    let row = sql.query_one("SELECT runtime_tenant_id,runtime_repository_id,snapshot_id FROM repository_bindings WHERE repository_id=$1", &[&repository.id]).unwrap();
    let mut runtime = DatabaseRuntime::connect(&config).unwrap();
    let snapshot = runtime
        .snapshot(
            &TenantId(row.get(0)),
            &RepositoryId(row.get(1)),
            Some(&SnapshotId(row.get(2))),
        )
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.sources.len(), 2);
    assert!(snapshot.sources.contains_key("source.js"));
    assert!(snapshot.sources.contains_key("source.lua"));
    let assignment = store
        .repository_analysis_profile(&ctx, &project.id, &repository.id)
        .unwrap();
    let report = &assignment["last_selection"];
    assert_eq!(report["policy"], "automatic-latest/v1");
    assert_eq!(report["profiles"].as_array().unwrap().len(), 2);
    assert!(
        report["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["version"] == "1.9.0")
    );
    assert!(
        report["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p.get("package").is_none())
    );
    assert!(
        report["uncovered_files"]
            .as_array()
            .unwrap()
            .contains(&json!("notes.unknown"))
    );
    assert_eq!(std::fs::read_dir(&managed).unwrap().count(), 0);
}
