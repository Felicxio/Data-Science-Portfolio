import httpx
import asyncio
from datetime import datetime, timedelta
import sys
import os

# Garante que o Python encontre o settings.py se você rodar de subpastas
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from settings import settings  

def _ensure_utf8_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

async def testar_conexao_pncp():
    # URL base conforme manual pág. 4 + Endpoint pág. 20
    # IMPORTANTE: No seu .env, a PNCP_BASE_URL deve ser "https://pncp.gov.br/api/consulta"
    endpoint = f"{settings.pncp_base_url}/v1/contratacoes/publicacao"
    
    # O manual exige data no formato AAAAMMDD
    # Usaremos a data de ontem para garantir que existam dados processados
    ontem = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    
    params = {
        "dataInicial": ontem,
        "dataFinal": ontem,
        "codigoModalidadeContratacao": 8, # 8 = Dispensa de Licitação (mais volume para teste)
        "pagina": 1,
        "tamanhoPagina": 10
    }

    # O servidor do governo exige o header 'accept'
    headers = {
        "accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Cursor/MechatronicsEngine"
    }

    print(f"📡 Tentando conexão em: {endpoint}")
    print(f"📅 Consultando data: {ontem}")
    
    async with httpx.AsyncClient(timeout=settings.pncp_timeout_seconds) as client:
        try:
            response = await client.get(endpoint, params=params, headers=headers)
            
            # Mapeamento de retornos conforme manual pág. 24
            if response.status_code == 200:
                dados = response.json()
                # Campo totalRegistros definido na pág. 6
                total = dados.get("totalRegistros", 0) 
                print(f"✅ Sucesso! Conexão estabelecida.")
                print(f"📊 Total de licitações encontradas para os filtros: {total}")
                
                # 'data' é o vetor de registros conforme pág. 6
                for item in dados.get("data", []):
                    # objetoCompra detalhado na pág. 22
                    objeto = item.get('objetoCompra', 'Sem descrição')
                    print(f"\n📦 Objeto: {objeto[:100]}...")
                    print(f"🔗 ID PNCP: {item.get('numeroControlePNCP')}")
            
            elif response.status_code == 204:
                print("info: Conexão OK, mas sem registros (204).")
            else:
                print(f"❌ Erro na API: {response.status_code}")
                print(f"Detalhes: {response.text}")

        except Exception as e:
            print(f"🚨 Erro de conexão: {e}")


if __name__ == "__main__":
    _ensure_utf8_stdout()
    asyncio.run(testar_conexao_pncp())