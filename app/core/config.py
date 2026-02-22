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

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
