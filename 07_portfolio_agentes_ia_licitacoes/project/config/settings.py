from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, HttpUrl

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # PNCP
    pncp_base_url: HttpUrl = Field("https://pncp.gov.br/api/consulta", description="URL base da API do PNCP")
    pncp_timeout_seconds: int = Field(30, ge=5, le=120)

    # Ollama
    ollama_base_url: HttpUrl = Field("http://localhost:11434")
    ollama_model: str = Field("llama3.1:8b")
    ollama_embed_model: str = Field("nomic-embed-text")

    # Segurança operacional
    max_concurrent_pdfs: int = Field(3, ge=1, le=10)
    max_tokens_per_chunk: int = Field(512, ge=128, le=2048)
    log_level: str = Field("INFO", pattern="^(DEBUG|INFO|WARNING|ERROR)$")

# Instância única — importada por todo o sistema
settings = Settings()