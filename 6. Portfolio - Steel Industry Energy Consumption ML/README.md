# ⚡ Previsão de Consumo Energético Industrial
### Sistema ML que economiza R$ 5.000+/ano por processo otimizado | 99.8% de acurácia

<div align="center">

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Status](https://img.shields.io/badge/STATUS-COMPLETO-success?style=for-the-badge)

**[📊 Ver Notebooks](./notebooks/)** • **[🤖 Modelo Treinado](./models/)** • **[📈 Resultados](./reports/)**

</div>

---

## 🎯 TL;DR (Resumo Executivo)

| | |
|---|---|
| **🎯 Objetivo** | Prever consumo energético industrial para reduzir custos operacionais |
| **📊 Dataset** | 35.041 registros de siderúrgica real (DAEWOO Steel Co.) |
| **🏆 Resultado** | Random Forest com **R² = 99.84%** e erro médio de **0.44 kWh** |
| **💰 Valor** | Simulação mostra economia de **R$ 5.000/ano** por processo otimizado |
| **⚙️ Stack** | Python, scikit-learn, XGBoost, LightGBM, Pandas, Feature Engineering |
| **⏱️ Tempo** | 3 semanas (EDA → Feature Engineering → Modelagem → Validação) |

---

## 💡 Por Que Este Projeto Importa?

### O Problema Real
Indústrias siderúrgicas gastam **30-40% dos custos operacionais** com energia elétrica. Sem previsão acurada:
- ❌ Cargas pesadas rodam em horários de tarifa alta (R$ 0.95/kWh vs R$ 0.35/kWh)
- ❌ Impossível negociar contratos diferenciados com concessionárias
- ❌ Desperdícios não detectados em tempo real

### A Solução
Sistema ML que **prevê consumo com 99.8% de acurácia**, permitindo:
- ✅ Agendar operações para horários de tarifa reduzida → **Economia imediata**
- ✅ Simular cenários "E se...?" antes de tomar decisões
- ✅ Detectar anomalias (consumo inesperado = possível falha de equipamento)

### Demonstração Prática
```
CENÁRIO: Mover carga "Maximum" de 10h (pico) → 3h (madrugada)

┌─────────────────┬────────────┬───────────────┬─────────────┐
│ Horário         │ Consumo    │ Tarifa        │ Custo       │
├─────────────────┼────────────┼───────────────┼─────────────┤
│ 10h (PICO)      │ 33.46 kWh  │ R$ 0.95/kWh   │ R$ 31.79    │
│ 3h (MADRUGADA)  │ 33.43 kWh  │ R$ 0.35/kWh   │ R$ 11.70    │
├─────────────────┴────────────┴───────────────┼─────────────┤
│ ECONOMIA POR OPERAÇÃO:                       │ R$ 20.09    │
│ ECONOMIA ANUAL (250 operações):              │ R$ 5.022    │
└──────────────────────────────────────────────┴─────────────┘

Modelo permite simular ANTES de executar!
```

---

## 📊 Resultados em Números

### Performance dos Modelos

<table>
<tr>
<th>Modelo</th>
<th>R² Score</th>
<th>MAE (kWh)</th>
<th>Interpretação</th>
</tr>
<tr>
<td><b>🏆 Random Forest</b></td>
<td><b>0.9984</b></td>
<td><b>0.44</b></td>
<td>Explica 99.84% da variação</td>
</tr>
<tr>
<td>XGBoost</td>
<td>0.9975</td>
<td>0.78</td>
<td>Segundo melhor</td>
</tr>
<tr>
<td>LightGBM</td>
<td>0.9971</td>
<td>0.85</td>
<td>Mais rápido</td>
</tr>
<tr>
<td>Linear (baseline)</td>
<td>0.8924</td>
<td>7.82</td>
<td>Baseline simples</td>
</tr>
</table>

### Validação Rigorosa
- ✅ **Validação Cruzada 5-fold:** Média R² = 0.9974 ± 0.0002 (modelo consistente!)
- ✅ **Sem data leakage:** Encoding calculado apenas em dados de treino
- ✅ **10 exemplos testados:** Erro médio absoluto de 0.25 kWh

---

## 🛠️ O Que Fiz (Competências Técnicas)

### 1️⃣ Análise Exploratória (EDA)
```
✓ Análise de 35k+ registros sem valores nulos
✓ Identificação de padrões temporais (pico 9-16h: 58 kWh vs madrugada: 4 kWh)
✓ Detecção e remoção de multicolinearidade (CO₂ correlação 0.99 com target)
✓ 10+ visualizações profissionais (distribuições, correlações, séries temporais)
```

### 2️⃣ Feature Engineering Orientado a Negócio
Criei **5 features estratégicas**:

| Feature | Técnica | Por Quê? | Impacto |
|---------|---------|----------|---------|
| `is_peak_operational` | Flag binária | Captura horário de alta operação (9-16h) | Modelo aprende padrão diário |
| `hour_sin` / `hour_cos` | Encoding cíclico | 23h está perto de 0h (não 23 unidades longe!) | +5% acurácia vs hora linear |
| `load_type_encoded` | Target encoding | Light=8.6, Medium=38.4, Maximum=59.3 kWh | Captura hierarquia natural |
| `is_weekend` | Flag binária | Fim de semana tem padrão diferente | Padrão operacional claro |

**Diferencial:** Cada feature tem **razão de negócio**, não só "tentativa e erro"

### 3️⃣ Modelagem com Rigor Científico
```
✓ Testei 5 algoritmos (baseline → state-of-the-art)
✓ Prevenção de data leakage (split ANTES de encoding)
✓ Validação cruzada para confirmar generalização
✓ Análise de feature importance (entender o "porquê")
✓ Documentação honesta de limitações
```

### 4️⃣ Pensamento Crítico
**Descobri:** Feature "Potência Reativa" domina com 88% de importância

**Reação amadora:** "Uau, R² = 99%! Sucesso!"  
**Minha reação:** "Por que tão alto? Há data leakage? É overfitting?"

**Investigação:**
- ✅ Validação cruzada confirma (não é overfitting)
- ✅ Correlação física legítima (Potência Reativa ↔ Potência Ativa)
- ⚠️ **Trade-off documentado:** Alta acurácia vs Dependência de sensores

**Conclusão:** Performance é real, mas modelo requer infraestrutura de medição em tempo real.

---

## 💼 Competências Demonstradas

<table>
<tr>
<td width="50%">

**Hard Skills**
- ✅ Python (Pandas, NumPy, scikit-learn)
- ✅ Machine Learning (RF, XGBoost, LightGBM)
- ✅ Feature Engineering avançado
- ✅ Validação cruzada e prevenção de overfitting
- ✅ Visualização de dados (Matplotlib, Seaborn)
- ✅ Git/GitHub (versionamento)
- ✅ Jupyter Notebooks

</td>
<td width="50%">

**Soft Skills**
- ✅ Pensamento analítico (detectar overfitting)
- ✅ Comunicação técnica (README claro)
- ✅ Orientação a negócio (simulações de ROI)
- ✅ Atenção a detalhes (data leakage)
- ✅ Documentação profissional
- ✅ Honestidade técnica (trade-offs)

</td>
</tr>
</table>

---

## 📁 Estrutura do Projeto (Organização Profissional)
```
📂 6. Portfolio - Steel Industry Energy Consumption ML/
│
├── 📓 notebooks/
│   ├── 01_EDA.ipynb                    ← Análise exploratória completa
│   ├── 02_Feature_Engineering.ipynb    ← 5 features criadas
│   └── 03_Modeling.ipynb               ← 5 modelos + validação
│
├── 💾 data/
│   ├── raw/                            ← Dataset original (35k registros)
│   └── processed/
│       ├── data_clean.csv              ← Pós-limpeza
│       └── data_with_features.csv      ← Com features prontas
│
├── 🤖 models/
│   └── best_model_rf.pkl               ← Random Forest treinado
│
├── 📊 reports/
│   └── model_comparison.csv            ← Comparação dos 5 modelos
│
└── 📄 README.md                        ← Este documento
```

**Tudo versionado no Git:** Commits descritivos, branches organizadas

---

## 🚀 Como Reproduzir (Para Recrutadores Técnicos)
```bash
# 1. Clonar repositório
git clone https://github.com/Felicxio/Data-Science-Portfolio.git
cd "Data-Science-Portfolio/6. Portfolio - Steel Industry Energy Consumption ML"

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Executar notebooks em ordem
jupyter notebook
# Abrir: 01_EDA.ipynb → 02_Feature_Engineering.ipynb → 03_Modeling.ipynb
```

**Tempo total:** ~15 minutos para ver todo o processo

---

## ⚠️ Transparência Técnica (Importante!)

### O Que Funciona Muito Bem
- ✅ Modelo prevê com erro médio de **0.44 kWh** (excelente!)
- ✅ Validação cruzada confirma consistência (não é "sorte")
- ✅ Simulações de negócio funcionam perfeitamente

### Limitação Documentada
**Feature "Potência Reativa" domina (88% importância)**

**O que isso significa:**
- ✅ Performance alta é **legítima** (relação física real)
- ⚠️ Modelo **depende de sensores** funcionando em tempo real
- ⚠️ Se medição falhar, modelo fica inoperante

**Solução proposta (próximos passos):**
- Treinar modelo "backup" sem potência reativa (R² ~92-95%, mais robusto)
- Implementar sistema de fallback automático

**Por que documentar isso?** Mostra maturidade técnica e pensamento crítico.

---

## 🎯 Principais Aprendizados

1. **Data leakage é sutil:** Calculei encoding ANTES de split (erro!) → Corrigi
2. **R² alto ≠ sempre bom:** Investigar o "porquê" é crucial
3. **Negócio > Métrica:** R² 99% impressiona, mas "economiza R$ 5k/ano" vende
4. **Validação é essencial:** Cross-validation revelou consistência real
5. **Documentação = diferencial:** 90% dos portfólios não explicam limitações

---

## 📞 Próximos Passos (Se Fosse Produção)

- [ ] **API REST** (FastAPI) para previsões em tempo real
- [ ] **Dashboard** interativo (Streamlit) para não-técnicos
- [ ] **Modelo robusto backup** sem potência reativa
- [ ] **Sistema de alertas** (Slack/email) para anomalias
- [ ] **A/B testing** em ambiente simulado
- [ ] **Monitoramento** de drift de dados

---

## 👤 Sobre Mim

**João Victor Assunção Pereira**  
Engenheiro Mecatrônico → Transição para Ciência de Dados

🎓 **Background:** Engenharia Mecatrônica, experiência com análise de dados industriais (estágio MBRF)  
💼 **Objetivo:** Cientista de Dados Júnior | Engenheiro de Dados Júnior  
🔧 **Diferenciais:** Conhecimento de domínio industrial + Rigor técnico em ML

📧 Email: [jvictor3651@gmail.com]  
💼 LinkedIn: [https://www.linkedin.com/in/joão-victor-assunção-pereira-88a461211]
🐙 GitHub: [github.com/Felicxio](https://github.com/Felicxio)

---

## 📌 Outros Projetos no Portfólio

1. **Classificação Spotify** - ML para classificação de músicas
2. **EDA YouTube** - Análise de tendências
3. **ETL Bitcoin** - Pipeline de dados
4. **Automação Python** - Otimização de processos
5. **ETL Northwind** - Modelagem de dados

➡️ **[Ver todos os projetos](https://github.com/Felicxio/Data-Science-Portfolio)**

---

<div align="center">

### ⭐ Gostou do projeto? Deixe uma estrela no repositório!

**Disponível para oportunidades de Ciência de Dados Júnior**

</div>

---

<sub>**Dataset:** DAEWOO Steel Co. via Kaggle | **Licença:** Projeto de portfólio pessoal</sub>
