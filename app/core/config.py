from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "kue-intelligence"
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    google_oauth_client_id: str = ""
    google_oauth_client_secret: str = ""
    google_oauth_redirect_uri: str = ""
    raw_events_db_path: str = "data/raw_events.db"
    canonical_events_db_path: str = "data/canonical_events.db"
    pipeline_db_path: str = "data/pipeline.db"
    parser_version: str = "v1"

    supabase_url: str = ""
    supabase_anon_key: str = ""
    supabase_service_role_key: str = ""
    supabase_raw_events_table: str = "raw_events"
    supabase_canonical_events_table: str = "canonical_events"

    inngest_event_key: str = ""
    inngest_signing_key: str = ""
    inngest_base_url: str = "https://inn.gs"
    inngest_source_app: str = "kue-intelligence"
    inngest_max_retries: int = 5
    alert_webhook_url: str = ""
    admin_reset_token: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
