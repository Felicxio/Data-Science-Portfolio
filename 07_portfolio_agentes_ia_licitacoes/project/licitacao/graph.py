"""
Grafo principal do sistema multi-agente.
Compilar com build_graph() e invocar com graph.ainvoke(estado_inicial).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from langgraph.graph import StateGraph, END

from licitacao.state import LicitacaoState
from licitacao.agents.ingestor import agente_ingestor


# ── Stubs dos agentes que serão implementados nos próximos sprints ───────────

async def agente_analista_tecnico(state: LicitacaoState) -> dict:
    """Sprint 2 — analisa PDFs dos editais filtrados."""
    return {"status": "analise_pendente", "erros": []}


async def agente_compliance(state: LicitacaoState) -> dict:
    """Sprint 3 — cruza requisitos com base de NRs."""
    return {"status": "compliance_pendente", "erros": []}


async def agente_relatorio(state: LicitacaoState) -> dict:
    """Sprint 4 — gera score e relatório final."""
    return {
        "status": "concluido",
        "viabilidade_score": None,
        "relatorio_final": {
            "total_encontrado": state.get("total_encontrado"),
            "total_engenharia": state.get("total_engenharia"),
            "contratos": state.get("contratos_filtrados", []),
        },
        "erros": [],
    }


# ── Roteadores condicionais ──────────────────────────────────────────────────

def rotear_apos_ingestao(state: LicitacaoState) -> str:
    """
    Aborta o pipeline se a ingestão não encontrou nada ou retornou erro.
    Retorna o nome do próximo nó.
    """
    if state.get("status") == "erro":
        return END

    total = state.get("total_engenharia", 0)
    if total == 0:
        return END

    return "analista_tecnico"


# ── Construção do grafo ──────────────────────────────────────────────────────

def build_graph() -> StateGraph:
    graph = StateGraph(LicitacaoState)

    graph.add_node("ingestor",          agente_ingestor)
    graph.add_node("analista_tecnico",  agente_analista_tecnico)
    graph.add_node("compliance",        agente_compliance)
    graph.add_node("relatorio",         agente_relatorio)

    graph.set_entry_point("ingestor")

    graph.add_conditional_edges(
        "ingestor",
        rotear_apos_ingestao,
        {
            "analista_tecnico": "analista_tecnico",
            END: END,
        },
    )

    graph.add_edge("analista_tecnico", "compliance")
    graph.add_edge("compliance",       "relatorio")
    graph.add_edge("relatorio",        END)

    return graph.compile()


# ── Entry point para execução direta ────────────────────────────────────────

async def executar(
    data_inicial: str | None = None,
    data_final: str | None = None,
    palavras_chave: list[str] | None = None,
    uf: str | None = None,
    modalidades: list[int] | None = None,
) -> dict:
    """
    Executa o pipeline completo com valores padrão sensatos.

    Exemplo:
        resultado = await executar(
            palavras_chave=["automação", "CLP"],
            uf="MG"
        )
    """
    hoje = datetime.now()
    estado_inicial: LicitacaoState = {
        "run_id": str(uuid.uuid4()),
        "status": "iniciado",
        "erros": [],

        "data_inicial": data_inicial or (hoje - timedelta(days=7)).strftime("%Y%m%d"),
        "data_final":   data_final   or hoje.strftime("%Y%m%d"),
        "palavras_chave":    palavras_chave or [],
        "modalidades_alvo":  modalidades or [4, 5, 6, 7, 8],
        "uf_filtro":         uf,

        "contratos_raw":       None,
        "contratos_filtrados": None,
        "total_encontrado":    None,
        "total_engenharia":    None,
        "pdfs_baixados":       [],

        "requisitos_tecnicos":  None,
        "normas_abnt_citadas":  None,
        "equipamentos_chave":   None,
        "prazo_execucao_dias":  None,

        "nrs_exigidas":    None,
        "gaps_compliance": None,
        "risco_tecnico":   None,

        "viabilidade_score": None,
        "relatorio_final":   None,
    }

    graph = build_graph()
    resultado = await graph.ainvoke(estado_inicial)
    return resultado


if __name__ == "__main__":
    import asyncio
    import logging
    from licitacao.tools.pncp_client import PNCPClient

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("licitacao.tools.pncp_client").setLevel(logging.DEBUG)

    async def main():
        hoje = datetime.now()
        data_final   = hoje.strftime("%Y%m%d")
        data_inicial = (hoje - timedelta(days=7)).strftime("%Y%m%d")

        # ── Passo 1: teste isolado — modalidade 8 ────────────────────────────
        print("=" * 60)
        print(f"PASSO 1 — Modalidade 8 isolada | {data_inicial} → {data_final}")
        print("=" * 60)

        client = PNCPClient()
        contratos, total = await client.buscar_contratacoes_publicacao(
            data_inicial=data_inicial,
            data_final=data_final,
            modalidades=[8],
            tamanho_pagina=50,
        )
        print(f"Resultado: {total} registros")
        for c in contratos[:3]:
            print(f"  [{c['numeroControlePNCP']}] {c['objetoCompra'][:70]}...")

        if total == 0:
            print("\n⚠ Nenhum resultado no passo 1.")
            print("  Possíveis causas:")
            print("  - API em rate limit (aguarde ~1 min e tente novamente)")
            print("  - Sem publicações neste período para esta modalidade")
            return

        # ── Passo 2: pipeline completo, modalidade 8, sem filtros extras ─────
        print("\n" + "=" * 60)
        print("PASSO 2 — Pipeline completo, modalidade 8, sem filtros")
        print("=" * 60)

        resultado = await executar(
            data_inicial=data_inicial,
            data_final=data_final,
            palavras_chave=[],
            uf=None,
            modalidades=[8],
        )

        print(f"Status:           {resultado['status']}")
        print(f"Total PNCP:       {resultado['total_encontrado']}")
        print(f"Total engenharia: {resultado['total_engenharia']}")
        print(f"Erros:            {resultado['erros']}")

        relatorio = resultado.get("relatorio_final") or {}
        contratos_filtrados = relatorio.get("contratos", [])
        print(f"\nPrimeiros 3 contratos de engenharia filtrados:")
        for c in contratos_filtrados[:3]:
            print(f"  [{c['numeroControlePNCP']}] {c['objetoCompra'][:70]}...")

    asyncio.run(main())