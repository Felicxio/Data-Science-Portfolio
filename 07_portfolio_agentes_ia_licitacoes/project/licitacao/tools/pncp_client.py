"""
Cliente assíncrono para a API de consultas do PNCP.
Referência: Manual das APIs do PNCP v1.0

Endpoint principal para engenharia:
  GET /v1/contratacoes/publicacao
  GET /v1/contratacoes/proposta   (propostas em aberto — mais útil para prospecção)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import settings

logger = logging.getLogger(__name__)


# ── Constantes do domínio (Manual seção 5) ──────────────────────────────────

# Seção 5.2 — Modalidades relevantes para engenharia (pág. 07)
MODALIDADES_ENGENHARIA = {
    4: "Concorrência - Eletrônica",
    5: "Concorrência - Presencial",
    6: "Pregão - Eletrônico",
    7: "Pregão - Presencial",
    8: "Dispensa de Licitação",
}

# Seção 5.11 — Categorias que indicam obra/engenharia na resposta (pág. 09)
CATEGORIAS_ENGENHARIA = {7, 9}  # 7=Obras, 9=Serviços de Engenharia

# Palavras-chave para filtro semântico no objetoCompra
KEYWORDS_ENGENHARIA = {
    # Automação e controle
    "automação", "clp", "scada", "ihm", "plc", "supervisório", "inversor",
    "servo", "robô", "robótico", "robótica", "controlador", "sistema linear",
    # Elétrica e potência
    "elétrico", "elétrica", "subestação", "trafo", "transformador",
    "quadro", "painel", "aterramento", "spda", "nr-10", "câmara de arco",
    # Mecânica e equipamentos
    "mecânico", "mecânica", "hidráulico", "pneumático", "compressor",
    "caldeira", "vaso de pressão", "tubulação", "junta de dilatação",
    # Obras e infraestrutura
    "obra", "construção", "reforma", "instalação industrial",
    "montagem", "manutenção preventiva", "manutenção corretiva", "painel de comando",
    # Normas regulamentadoras
    "nr-01", "nr-06", "nr-12", "nr-13", "nr-15", "nr-16",
    "nr-18", "nr-21", "nr-33", "nr-35", "abnt", "nbr",
}

"""
Referência das NRs mapeadas:
NR-01  Disposições Gerais e Gerenciamento de Riscos Ocupacionais (GRO/PGR)
NR-06  Equipamentos de Proteção Individual (EPI)
NR-10  Segurança em Instalações e Serviços em Eletricidade
NR-12  Segurança no Trabalho em Máquinas e Equipamentos
NR-13  Caldeiras, Vasos de Pressão e Tubulações
NR-15  Atividades e Operações Insalubres
NR-16  Atividades e Operações Perigosas
NR-18  Segurança e Saúde na Indústria da Construção
NR-33  Segurança e Saúde em Espaços Confinados
NR-35  Trabalho em Altura
"""


class PNCPClient:
    """
    Cliente para a API de Consultas do PNCP.
    Gerencia paginação automática, retry com backoff e filtragem por domínio.
    """

    BASE_URL = str(settings.pncp_base_url)

    def __init__(self) -> None:
        self._headers = {
            "accept": "application/json",
            "User-Agent": (
                "LicitacaoInteligente/1.0 "
                "(+https://github.com/Felicxio/Data-Science-Portfolio)"
            ),
        }

    @retry(
        retry=retry_if_exception_type((
            httpx.TimeoutException,
            httpx.ReadTimeout,    # captura ReadTimeout explicitamente
            httpx.ConnectError,
        )),
        wait=wait_exponential(multiplier=2, min=3, max=60),  # backoff mais agressivo
        stop=stop_after_attempt(5),   # era 3 — mais chances antes de desistir
        reraise=True,
    )
    async def _get(
        self, client: httpx.AsyncClient, endpoint: str, params: dict
    ) -> dict:
        """
        GET com retry automático em falhas de rede e timeout.

        Retorna {} nos casos:
          - 204 No Content (sem registros — esperado pela API do PNCP)
          - body vazio inesperado em outras respostas 2xx

        Lança httpx.HTTPStatusError para 4xx/5xx, permitindo que o
        chamador decida como tratar cada código.
        """
        response = await client.get(
            f"{self.BASE_URL}{endpoint}",
            params=params,
            headers=self._headers,
        )

        # 204 = sucesso sem corpo — não tenta parsear JSON
        if response.status_code == 204:
            return {}

        response.raise_for_status()

        # Guarda contra body vazio inesperado em outras respostas 2xx
        if not response.content:
            return {}

        return response.json()

    async def buscar_contratacoes_publicacao(
        self,
        data_inicial: str,
        data_final: str,
        modalidades: list[int] | None = None,
        uf: str | None = None,
        tamanho_pagina: int = 50,
    ) -> tuple[list[dict], int]:
        """
        Endpoint 6.3 (pág. 20) do manual: Consultar Contratações por Data de Publicação.

        Faz paginação automática — retorna TODOS os registros do período,
        não apenas a primeira página.

        Args:
            data_inicial:   formato AAAAMMDD
            data_final:     formato AAAAMMDD
            modalidades:    lista de códigos (seção 5.2). None = todas as de engenharia
            uf:             sigla da UF ou None para nacional
            tamanho_pagina: máx 500 conforme manual (default 50 para este endpoint)

        Returns:
            (lista de contratos, total de registros encontrados)
        """
        if modalidades is None:
            modalidades = list(MODALIDADES_ENGENHARIA.keys())

        todos_contratos: list[dict] = []
        total_registros = 0

        async with httpx.AsyncClient(
            timeout=settings.pncp_timeout_seconds,
            follow_redirects=True,
        ) as client:
            for modalidade in modalidades:

                # Pausa entre modalidades para não acionar rate limiting do PNCP
                await asyncio.sleep(1.0)

                pagina = 1
                paginas_restantes = 1  # entra no loop pelo menos uma vez

                while paginas_restantes > 0:
                    params: dict[str, Any] = {
                        "dataInicial": data_inicial,
                        "dataFinal": data_final,
                        "codigoModalidadeContratacao": modalidade,
                        "pagina": pagina,
                        "tamanhoPagina": tamanho_pagina,
                    }
                    if uf:
                        params["uf"] = uf

                    try:
                        dados = await self._get(
                            client, "/v1/contratacoes/publicacao", params
                        )
                    except httpx.ReadTimeout:
                        # Servidor sobrecarregado — aguarda e passa para próxima modalidade
                        logger.warning(
                            "ReadTimeout na modalidade=%s pagina=%s — "
                            "servidor sobrecarregado, aguardando 10s antes de continuar",
                            modalidade, pagina,
                        )
                        await asyncio.sleep(10.0)
                        break
                    except httpx.HTTPStatusError as exc:
                        status = exc.response.status_code
                        if status == 400:
                            # Modalidade sem dados no período — comportamento normal
                            logger.debug(
                                "Modalidade %s sem dados no período (400)", modalidade
                            )
                        else:
                            logger.warning(
                                "Erro HTTP modalidade=%s pagina=%s: %s",
                                modalidade, pagina, status,
                            )
                        break

                    # _get retorna {} em caso de 204 ou body vazio — encerra paginação
                    if not dados:
                        break

                    registros = dados.get("data", [])
                    todos_contratos.extend(registros)
                    total_registros += len(registros)

                    paginas_restantes = dados.get("paginasRestantes", 0)
                    pagina += 1

                    # Rate limiting defensivo entre páginas da mesma modalidade
                    if paginas_restantes > 0:
                        await asyncio.sleep(2.0)   # era 0.5 — aumentado para evitar timeout

                logger.info(
                    "Modalidade %s (%s): %d registros acumulados",
                    modalidade,
                    MODALIDADES_ENGENHARIA.get(modalidade, "?"),
                    len(todos_contratos),
                )

        return todos_contratos, total_registros

    async def buscar_propostas_em_aberto(
        self,
        data_final: str,
        modalidades: list[int] | None = None,
        uf: str | None = None,
    ) -> list[dict]:
        """
        Endpoint 6.4 do manual: Contratações com Período de Recebimento em Aberto.
        Mais útil para prospecção ativa — retorna licitações que ainda aceitam propostas.
        """
        if modalidades is None:
            modalidades = list(MODALIDADES_ENGENHARIA.keys())

        resultados: list[dict] = []

        async with httpx.AsyncClient(
            timeout=settings.pncp_timeout_seconds,
            follow_redirects=True,
        ) as client:
            for modalidade in modalidades:

                # Pausa entre modalidades para não acionar rate limiting
                await asyncio.sleep(1.0)

                pagina = 1
                paginas_restantes = 1

                while paginas_restantes > 0:
                    params: dict[str, Any] = {
                        "dataFinal": data_final,
                        "codigoModalidadeContratacao": modalidade,
                        "pagina": pagina,
                        "tamanhoPagina": 500,
                    }
                    if uf:
                        params["uf"] = uf

                    try:
                        dados = await self._get(
                            client, "/v1/contratacoes/proposta", params
                        )
                    except httpx.ReadTimeout:
                        logger.warning(
                            "ReadTimeout propostas modalidade=%s pagina=%s — "
                            "aguardando 10s antes de continuar",
                            modalidade, pagina,
                        )
                        await asyncio.sleep(10.0)
                        break
                    except httpx.HTTPStatusError as exc:
                        status = exc.response.status_code
                        if status == 400:
                            logger.debug(
                                "Propostas modalidade %s sem dados (400)", modalidade
                            )
                        else:
                            logger.warning(
                                "Erro HTTP propostas modalidade=%s: %s",
                                modalidade, status,
                            )
                        break

                    # Sem conteúdo — encerra paginação desta modalidade
                    if not dados:
                        break

                    resultados.extend(dados.get("data", []))
                    paginas_restantes = dados.get("paginasRestantes", 0)
                    pagina += 1

                    if paginas_restantes > 0:
                        await asyncio.sleep(2.0)   # era 0.5

        return resultados

    async def buscar_documentos_contratacao(
        self, numero_controle_pncp: str
    ) -> list[dict]:
        """
        Retorna todos os documentos (PDFs) de uma contratação específica.
        Endpoint: GET /v1/orgaos/{cnpj}/compras/{ano}/{seq}/documentos
        Referência: Manual de Integração PNCP seção 6.3.8

        Args:
            numero_controle_pncp: ex. "07954480000179-1-005569/2026"
        """
        partes = numero_controle_pncp.split("-")
        if len(partes) < 3:
            raise ValueError(
                f"numeroControlePNCP inválido: {numero_controle_pncp!r}"
            )

        cnpj = partes[0]
        seq_ano = partes[2]  # "005569/2026"

        if "/" not in seq_ano:
            raise ValueError(
                f"Formato de sequencial/ano inválido em: {numero_controle_pncp!r}"
            )

        sequencial, ano = seq_ano.split("/")
        endpoint = f"/v1/orgaos/{cnpj}/compras/{ano}/{sequencial}/documentos"

        async with httpx.AsyncClient(
            timeout=settings.pncp_timeout_seconds,
            follow_redirects=True,
        ) as client:
            try:
                dados = await self._get(client, endpoint, {})
                if not dados:
                    return []
                return dados if isinstance(dados, list) else dados.get("data", [])
            except httpx.ReadTimeout:
                logger.warning(
                    "ReadTimeout ao buscar documentos de %s", numero_controle_pncp
                )
                return []
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "Sem documentos para %s: HTTP %s",
                    numero_controle_pncp,
                    exc.response.status_code,
                )
                return []


def filtrar_engenharia(contratos: list[dict]) -> list[dict]:
    """
    Filtro de domínio aplicado sobre a lista bruta da API.
    Duas estratégias combinadas:

    1. categoriaProcesso.id ∈ {7, 9}  — sinalização explícita de obra/engenharia
    2. palavras-chave no objetoCompra  — captura casos onde a categoria não foi
       preenchida corretamente pelo órgão publicador (comum em municípios menores)

    Retorna apenas contratos que passam em pelo menos um dos critérios.
    """
    filtrados = []

    for contrato in contratos:
        # Critério 1: categoria explícita do processo
        categoria = contrato.get("categoriaProcesso", {})
        if isinstance(categoria, dict) and categoria.get("id") in CATEGORIAS_ENGENHARIA:
            filtrados.append(contrato)
            continue

        # Critério 2: keywords no objeto — case-insensitive
        objeto = contrato.get("objetoCompra", "").lower()
        if any(kw in objeto for kw in KEYWORDS_ENGENHARIA):
            filtrados.append(contrato)

    return filtrados