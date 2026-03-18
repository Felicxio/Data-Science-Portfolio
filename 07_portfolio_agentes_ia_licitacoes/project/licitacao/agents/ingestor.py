"""
Agente Ingestor — nó 1 do grafo LangGraph.

Responsabilidades:
  1. Chamar a API do PNCP com os parâmetros do estado
  2. Aplicar o filtro de engenharia nos resultados
  3. Retornar os campos atualizados do estado (nunca modifica o estado diretamente)
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

from licitacao.state import LicitacaoState, ContratoRaw
from licitacao.tools.pncp_client import PNCPClient, filtrar_engenharia, MODALIDADES_ENGENHARIA

logger = logging.getLogger(__name__)


async def agente_ingestor(state: LicitacaoState) -> dict:
    """
    Nó LangGraph: deve retornar um dict com apenas os campos que modifica.
    O framework faz o merge automático no estado global.
    """
    logger.info("[Ingestor] Iniciando busca no PNCP | run_id=%s", state["run_id"])

    client = PNCPClient()

    try:
        contratos_raw, total = await client.buscar_contratacoes_publicacao(
            data_inicial=state["data_inicial"],
            data_final=state["data_final"],
            modalidades=state.get("modalidades_alvo"),
            uf=state.get("uf_filtro"),
        )
    except Exception as exc:
        logger.error("[Ingestor] Falha na API do PNCP: %s", exc)
        return {
            "status": "erro",
            "erros": [f"Ingestor: falha na API PNCP — {exc}"],
            "contratos_raw": [],
            "contratos_filtrados": [],
            "total_encontrado": 0,
            "total_engenharia": 0,
        }

    # Aplica filtro de domínio de engenharia
    filtrados = filtrar_engenharia(contratos_raw)

    # Aplica filtro adicional por palavras-chave do usuário (se fornecidas)
    palavras = [p.lower() for p in state.get("palavras_chave", [])]
    if palavras:
        filtrados = [
            c for c in filtrados
            if any(p in c.get("objetoCompra", "").lower() for p in palavras)
        ]

    logger.info(
        "[Ingestor] Total API: %d | Engenharia: %d | Após filtro usuario: %d",
        total,
        len(filtrar_engenharia(contratos_raw)),  # antes do filtro do usuário
        len(filtrados),
    )

    return {
        "status": "ingestao_concluida",
        "contratos_raw": contratos_raw,       # type: ignore[typeddict-item]
        "contratos_filtrados": filtrados,      # type: ignore[typeddict-item]
        "total_encontrado": total,
        "total_engenharia": len(filtrados),
        "erros": [],
    }