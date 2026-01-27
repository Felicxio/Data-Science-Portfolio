# 📊 Automação de Dados com Python (Excel → Email)

## 📌 Descrição
Projeto simples de automação utilizando Python e Pandas.  
O script lê uma planilha Excel de vendas, realiza transformações nos dados
e envia automaticamente um relatório formatado por email via Outlook.

## ⚙️ Funcionalidades
- Leitura de arquivo Excel
- Agrupamento de dados por loja
- Cálculo de faturamento, quantidade vendida e ticket médio
- Envio automático de email com tabelas formatadas em HTML

## 🛠️ Tecnologias Utilizadas
- Python
- Pandas
- OpenPyXL
- PyWin32 (integração com Outlook)

O envio de email funciona apenas em ambiente Windows com Outlook configurado.
## 📁 Estrutura
```text
├── MeuArquivo.py
├── Vendas.xlsx
├── requirements.txt

