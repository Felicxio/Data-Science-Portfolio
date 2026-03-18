from __future__ import annotations
import operator
from typing import TypedDict, Annotated, Literal, Final, Optional


class ContratoRaw(TypedDict):
    """Representação bruta de um contrato, como retornado pela API do PNCP."""
    numeroControlePNCP: str
    objetoCompra: str
    valorTotalEstimado: float
    dataPublicacaoPncp: str
    dataEncerramentoProposta: Optional[str]
    modalidadeNome: str
    situacaoCompraNome: str
    orgaoEntidade: dict
    unidadeOrgao: dict
    linkSistemaOrigem: Optional[str]

class LicitacaoState(TypedDict):
    # ── Controle do pipeline ─────────────────────────────────────────────────
    run_id: str                                        # UUID gerado no entry point
    status: str                                        # "iniciado" | "ingestao" | "analise" | "compliance" | "concluido" | "erro"
    erros: Annotated[list[str], operator.add]          # acumulativo — cada agente faz append

    # ── Parâmetros de busca (entrada do usuário) ─────────────────────────────
    data_inicial: str                                  # formato AAAAMMDD
    data_final: str                                    # formato AAAAMMDD
    palavras_chave: list[str]                          # ex: ["automação", "CLP", "elétrica"]
    modalidades_alvo: list[int]                        # códigos do manual — default: [4,5,6]
    uf_filtro: Optional[str]                           # ex: "MG" — None = nacional

    # ── Saída do Agente Ingestor ─────────────────────────────────────────────
    contratos_raw: Optional[list[ContratoRaw]]         # resposta bruta da API
    contratos_filtrados: Optional[list[ContratoRaw]]   # após filtro de engenharia
    total_encontrado: Optional[int]
    total_engenharia: Optional[int]
    pdfs_baixados: Annotated[list[str], operator.add]  # paths locais dos PDFs

    # ── Saída do Agente Analista Técnico ─────────────────────────────────────
    requisitos_tecnicos: Optional[list[dict]]
    normas_abnt_citadas: Optional[list[str]]
    equipamentos_chave: Optional[list[str]]
    prazo_execucao_dias: Optional[int]

    # ── Saída do Agente de Compliance ────────────────────────────────────────
    nrs_exigidas: Optional[list[str]]
    gaps_compliance: Optional[list[dict]]
    risco_tecnico: Optional[str]                       # "baixo" | "medio" | "alto"

    # ── Relatório final ──────────────────────────────────────────────────────
    viabilidade_score: Optional[float]    
    relatorio_final: Optional[dict]