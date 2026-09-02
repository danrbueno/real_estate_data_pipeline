# AI Scraper - Guia de Otimização e Boas Práticas

## 🎯 Otimizações de Performance

### 1. Selecionar Modelo Adequado

```python
# Em config.py
# Para máxima inteligência (mais caro)
OPENAI_MODEL = "gpt-4-turbo"           # $0.01 por 1K tokens

# Para balanço custo/qualidade
OPENAI_MODEL = "gpt-3.5-turbo"         # $0.0005 por 1K tokens

# Para testes rápidos (mais barato)
OPENAI_MODEL = "gpt-3.5-turbo-instruct" # Mais rápido
```

**Recomendação:** Usar `gpt-3.5-turbo` para produção (melhor relação custo-benefício).

### 2. Ajustar Rate Limiting

```python
# config.py
REQUEST_DELAY = 1  # Mínimo (risco de rate limit)
REQUEST_DELAY = 2  # Recomendado
REQUEST_DELAY = 5  # Seguro (para sites sensíveis)
```

### 3. Limitar Número de Páginas

```bash
# Testar com poucas páginas primeiro
python ai_scraper/main.py --type rentals --max-pages 3

# Em produção
python ai_scraper/main.py --type rentals  # Todas as páginas
```

### 4. Implementar Cache

```python
# Em ai_scraper/scraper.py
import hashlib

class AIScraper:
    def __init__(self):
        # ... código existente ...
        self.cache = {}
    
    def get_cached_or_fetch(self, url):
        cache_key = hashlib.md5(url.encode()).hexdigest()
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        html = self.http_client.get(url)
        self.cache[cache_key] = html
        return html
```

## 📊 Monitoramento

### 1. Logging Estruturado

```python
# Em ai_scraper/scraper.py
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info(f"Processando página {current_page}")
```

### 2. Métricas OpenAI

```python
# Rastrear tokens e custo
def calculate_cost(tokens_used, model):
    if model == "gpt-4-turbo":
        return tokens_used * 0.00001  # $0.01 por 1K tokens
    elif model == "gpt-3.5-turbo":
        return tokens_used * 0.0000005  # $0.0005 por 1K tokens

# Usar response.usage.total_tokens
print(f"Tokens used: {response.usage.total_tokens}")
print(f"Estimated cost: ${calculate_cost(response.usage.total_tokens, 'gpt-3.5-turbo')}")
```

## 🔐 Segurança

### 1. Proteger API Key

✅ **Fazer:**
```python
# .env
OPENAI_API_KEY=sk-...

# config.py
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
```

❌ **NÃO fazer:**
```python
# Nunca hardcode a chave
OPENAI_API_KEY = "sk-..."

# Nunca commite .env
# Adicionar ao .gitignore
echo ".env" >> .gitignore
```

### 2. Validar Dados

```python
def validate_extraction(self, data):
    # Verificar campos obrigatórios
    required = ["title", "link"]
    if not all(k in data for k in required):
        return False
    
    # Verificar tipo de dados
    if not isinstance(data.get("title"), str):
        return False
    
    # Verificar URL válida
    if not data.get("link", "").startswith("http"):
        return False
    
    return True
```

### 3. Rate Limiting

```python
# Respeitar limites de API
MAX_REQUESTS_PER_MINUTE = 20

# Implementado em http_client.py
time.sleep(REQUEST_DELAY)
```

## 🚀 Deploy em Produção

### 1. Variáveis de Ambiente

```bash
# .env.production
OPENAI_API_KEY=sk-prod-...
OPENAI_MODEL=gpt-3.5-turbo
REQUEST_TIMEOUT=60
MAX_PAGES=null  # Scrape todas
REQUEST_DELAY=2
```

### 2. Configuração Airflow

```python
# airflow/dags/dag_pipeline_real_estate_ai.py
task_scrap_rentals = PythonOperator(
    task_id="scrap_rentals",
    python_callable=scrap_rentals,
    retries=2,
    retry_delay=timedelta(minutes=5),
    pool="scraping",  # Limitar concorrência
)
```

### 3. Monitoramento em Produção

```python
# Adicionar ao DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

task_notify_completion = SlackWebhookOperator(
    task_id="notify_slack",
    http_conn_id="slack",
    message="Scraping completed: {{ task_instance.xcom_pull(task_ids='scrap_rentals') }}"
)
```

## 🐛 Debug e Troubleshooting

### 1. Ativar Debug Mode

```python
# ai_scraper/config.py
DEBUG = True

# Use em ai_scraper/scraper.py
if DEBUG:
    print(f"Fetching: {url}")
    print(f"HTML length: {len(html)}")
```

### 2. Salvar Respostas para Análise

```python
# Salvar HTML de páginas problemáticas
import json

def save_debug_response(url, html, response):
    debug_file = f"debug_{datetime.now().timestamp()}.json"
    with open(debug_file, 'w') as f:
        json.dump({
            "url": url,
            "html_length": len(html),
            "response": response,
            "timestamp": datetime.now().isoformat()
        }, f)
```

### 3. Testes Unitários

```python
# tests/test_ai_agent.py
import unittest

class TestAIAgent(unittest.TestCase):
    def setUp(self):
        self.agent = AIScrapingAgent()
    
    def test_extract_links(self):
        # Mock HTML
        html = '<a href="/property/123">Property</a>'
        links = self.agent.extract_property_links(html, "https://example.com")
        self.assertIn("https://example.com/property/123", links)
```

## 💡 Dicas Práticas

### 1. Testar com Páginas Pequenas Primeiro

```bash
# Teste com 1 página
python ai_scraper/main.py --type rentals --max-pages 1

# Depois 5
python ai_scraper/main.py --type rentals --max-pages 5

# Depois todas
python ai_scraper/main.py --type rentals
```

### 2. Usar Diferentes Modelos por Tarefa

```python
# Para listagens (prompt simples)
OPENAI_MODEL = "gpt-3.5-turbo"

# Para detalhes (prompt complexo)
OPENAI_MODEL = "gpt-4-turbo"
```

### 3. Implementar Fallback

```python
def extract_property_links(self, html, base_url):
    try:
        # Tentar com IA
        return self._call_openai(prompt)
    except Exception as e:
        logger.warning(f"AI extraction failed, using fallback: {e}")
        # Fallback para BeautifulSoup
        return self._extract_links_bs4(html, base_url)
```

## 📈 Escalabilidade

### 1. Paralelizar Requisições

```python
from concurrent.futures import ThreadPoolExecutor

def scrape_properties_parallel(property_links):
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(
            self.scrape_property,
            property_links
        ))
    return results
```

### 2. Usar Queue para Distribuição

```python
from celery import Celery

app = Celery('scraper')

@app.task
def scrape_property(url):
    return AIScrapingAgent().extract_property_details(...)
```

## 📚 Referências

- [OpenAI API Best Practices](https://platform.openai.com/docs/guides/tokens)
- [Rate Limiting Strategies](https://platform.openai.com/docs/guides/rate-limits)
- [Python Async/Await](https://docs.python.org/3/library/asyncio.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

---

**Última atualização:** 2024
**Versão:** 1.0
