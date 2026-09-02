# Migração: Scrapy → AI Scraper com OpenAI

## 📋 Resumo da Migração

Este projeto foi atualizado para usar **agentes de IA da OpenAI** em vez do Scrapy tradicional para fazer web scraping de dados de imóveis.

## 🔄 O que Mudou

### Antes (Scrapy)
```
scrapy/
├── scrap.py                 # Script principal Scrapy
├── scraping/
│   ├── spiders/
│   │   └── DFImoveis.py    # Spider com seletores CSS
│   ├── items.py            # Definição de items
│   ├── pipelines.py        # Salvamento em JSON
│   └── settings.py         # Configurações Scrapy
```

### Agora (AI Scraper)
```
ai_scraper/
├── __init__.py             # Package initialization
├── config.py               # Configurações
├── http_client.py          # Cliente HTTP com rate limiting
├── ai_agent.py             # Agente OpenAI
├── scraper.py              # Orquestrador principal
├── main.py                 # Script CLI
└── requirements.txt        # Dependências
```

## 🚀 Setup Rápido

### 1. Instalar Dependências

```bash
cd c:\repos\real_estate_data_pipeline

# Instalar dependências do AI Scraper
pip install -r ai_scraper/requirements.txt

# Se ainda usar Airflow, instalar também
pip install apache-airflow
```

### 2. Configurar OpenAI API Key

```bash
# Criar arquivo .env
cp .env.example .env

# Editar .env e adicionar sua API key
# OPENAI_API_KEY=sk-...
```

### 3. Usar o AI Scraper

**Opção A: Linha de Comando**
```bash
# Scrape rentals
python ai_scraper/main.py --type rentals

# Scrape sales
python ai_scraper/main.py --type sales
```

**Opção B: Em Python**
```python
from ai_scraper import AIScraper

scraper = AIScraper()
scraper.scrape_transaction_type("rentals")
scraper.close()
```

**Opção C: Com Airflow (novo DAG)**
```bash
# Usar o novo DAG com AI Scraper
airflow dags test dag_real_estate_data_pipeline_ai 2024-01-01
```

## 📊 Comparação de Funcionalidades

| Funcionalidade | Scrapy | AI Scraper |
|---|---|---|
| **Extração de Dados** | Seletores CSS rígidos | Compreensão semântica |
| **Adaptabilidade** | Requer mudanças de código | Funciona com layouts variados |
| **Manutenção** | Quebra quando site muda | Adaptativo a mudanças |
| **Inteligência** | Nenhuma | GPT-4 com análise de conteúdo |
| **Paginação** | Manual com lógica CSS | Automática e inteligente |
| **Extração de Detalhes** | Seletores específicos | Extrai todos os dados relevantes |
| **Performance** | Rápido | Mais lento (chamadas de API) |
| **Custo** | Nenhum | Custa créditos OpenAI |

## 💰 Custo OpenAI Estimado

Usando **gpt-4-turbo**:
- ~50 propriedades por dia
- ~$0.50 - $1.00 por dia (estimado)
- ~$15 - $30 por mês

Você pode usar `gpt-3.5-turbo` para reduzir custos (mais rápido e barato):
```python
# Em ai_scraper/config.py
OPENAI_MODEL = "gpt-3.5-turbo"  # ~10x mais barato
```

## 🔧 Integração com Airflow

### DAG Antigo (Scrapy)
```python
task_scrap_rentals = BashOperator(
    task_id="scrap_rentals",
    bash_command="cd "+scrapy_dir+" && scrapy crawl DFImoveis -a transaction_type=rentals",
)
```

### DAG Novo (AI Scraper)
```python
task_scrap_rentals = PythonOperator(
    task_id="scrap_rentals",
    python_callable=scrap_rentals,  # Função Python
    retries=2,
)
```

**O novo DAG está em:** `airflow/dags/dag_pipeline_real_estate_ai.py`

## 📂 Estrutura de Saída (Compatível)

A saída JSON é **100% compatível** com o pipeline de transformação existente:

```json
{
  "title": "Apartamento 2 quartos",
  "link": "https://...",
  "price": "R$ 250.000",
  "bedrooms": "2",
  "bathrooms": "1",
  "area": "80 m²",
  "neighborhood": "Asa Sul",
  "scraped_at": "2024-01-15 10:30:45"
}
```

Os arquivos continuam em:
- `data/web/rentals.json`
- `data/web/sales.json`

## 🛠️ Troubleshooting

### ❌ "No OPENAI_API_KEY found"
```bash
# Verificar se .env existe
ls -la .env

# Recriar se necessário
cp .env.example .env
```

### ❌ "rate_limit_exceeded"
Aumentar delay entre requisições em `ai_scraper/config.py`:
```python
REQUEST_DELAY = 3  # de 2 para 3 segundos
```

### ❌ "Extraction failed"
Aumentar token limit ou simplificar prompt. Verificar se site está acessível.

## 🔀 Migração Gradual

Se preferir migrar gradualmente:

1. **Manter ambos os DAGs funcionando** (Scrapy e AI Scraper em paralelo)
2. **Comparar resultados** por 1-2 semanas
3. **Ajustar configurações** conforme necessário
4. **Desabilitar Scrapy** após validar AI Scraper

## 📝 Próximos Passos

- [ ] Testar com dados reais
- [ ] Ajustar prompts se necessário
- [ ] Validar qualidade de extração
- [ ] Otimizar custos OpenAI
- [ ] Adicionar logging estruturado
- [ ] Implementar cache de resultados
- [ ] Adicionar suporte para mais sites

## 📖 Referências

- [OpenAI API Docs](https://platform.openai.com/docs)
- [Apache Airflow Docs](https://airflow.apache.org/docs)
- [Projeto original](README.md)

## ❓ Perguntas Frequentes

**P: O AI Scraper é mais rápido?**
R: Não, é mais lento. Mas é mais inteligente e requer menos manutenção.

**P: Posso usar modelos gratuitos?**
R: Não, você precisa de API key paga OpenAI. Mas o custo é baixo (~$15-30/mês).

**P: Funciona offline?**
R: Não, precisa conectividade com OpenAI API.

**P: E se o site mudar de layout?**
R: O AI Scraper se adapta automaticamente. Scrapy quebraria.

---

**Implementado em:** 2024
**Versão:** 1.0
**Modelo AI:** GPT-4 Turbo
