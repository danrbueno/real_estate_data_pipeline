# AI Scraper - OpenAI Powered Web Scraping

Substitui o Scrapy tradicional por agentes de IA da OpenAI para extrair dados de imóveis do site DFImoveis.

## ✨ Vantagens sobre Scrapy

- **Inteligência Adaptativa**: Agentes de IA entendem conteúdo dinâmico e layouts variados
- **Melhor Extração**: Compreensão semântica dos dados em vez de parsing CSS rígido
- **Sem Manutenção de Seletores**: Não precisa atualizar seletores CSS quando o site muda
- **Escalável**: Facilmente adaptável para novos sites sem reconfigurações
- **Inteligência de Paginação**: Compreende automaticamente estrutura de paginação

## 🔧 Instalação

### 1. Instalar Dependências

```bash
pip install -r ai_scraper/requirements.txt
```

### 2. Configurar OpenAI API Key

Criar arquivo `.env` na raiz do projeto:

```bash
cp .env.example .env
```

Editar `.env` e adicionar sua API key da OpenAI:

```
OPENAI_API_KEY=sk-...seu-api-key...
OPENAI_MODEL=gpt-4-turbo
```

## 🚀 Uso

### Linha de Comando

```bash
# Scrape rentals
python ai_scraper/main.py --type rentals

# Scrape sales
python ai_scraper/main.py --type sales

# Com limite de páginas
python ai_scraper/main.py --type rentals --max-pages 5
```

### Em Código Python

```python
from ai_scraper import AIScraper

scraper = AIScraper()
properties = scraper.scrape_transaction_type("rentals")
scraper.close()

print(f"Extracted {len(properties)} properties")
```

### Com Airflow (DAG)

```python
from ai_scraper import AIScraper

def scrape_rentals():
    scraper = AIScraper()
    properties = scraper.scrape_transaction_type("rentals")
    scraper.close()
    return len(properties)

def scrape_sales():
    scraper = AIScraper()
    properties = scraper.scrape_transaction_type("sales")
    scraper.close()
    return len(properties)
```

## 📊 Estrutura de Saída

Os dados são salvos em JSON (mesma estrutura do Scrapy):

**`data/web/rentals.json`** e **`data/web/sales.json`**

```json
{
  "title": "Apartamento 2 quartos em Brasília",
  "link": "https://www.dfimoveis.com.br/...",
  "price": "R$ 250.000",
  "bedrooms": "2",
  "bathrooms": "1",
  "area": "80 m²",
  "neighborhood": "Asa Sul",
  "scraped_at": "2024-01-15 10:30:45",
  "other_features": {...}
}
```

## 🤖 Como Funciona

1. **Fetch da Página**: Baixa HTML da página de listagem
2. **Extração de Links**: IA identifica todos os links de imóveis
3. **Paginação**: IA compreende estrutura de pagination e navega automaticamente
4. **Detalhes do Imóvel**: Para cada imóvel, IA extrai:
   - Título e preço
   - Características (quartos, banheiros, área, etc.)
   - Localização e bairro
   - Descrição e amenidades
   - Todos os outros dados visíveis
5. **Salvamento**: Dados salvos em JSON, um item por linha

## 📝 Arquitetura

```
ai_scraper/
├── __init__.py           # Package initialization
├── config.py             # Configuration and constants
├── http_client.py        # HTTP requests with rate limiting
├── ai_agent.py           # OpenAI AI agent for data extraction
├── scraper.py            # Main orchestrator
├── main.py               # CLI entry point
└── requirements.txt      # Dependencies
```

## ⚙️ Configuração Avançada

Editar `ai_scraper/config.py`:

```python
# Modelo OpenAI (padrão: gpt-4-turbo)
OPENAI_MODEL = "gpt-4-turbo"

# Timeout para requisições (segundos)
REQUEST_TIMEOUT = 30

# Delay entre requisições (segundos)
REQUEST_DELAY = 2

# Máximo de páginas a processar (None = todas)
MAX_PAGES = None
```

## 🔒 Segurança

- API key armazenada em `.env` (nunca commitado)
- Rate limiting automático
- User-Agent configurado
- Tratamento de erros robusto

## 💡 Próximos Passos

1. Integrar com DAG do Airflow
2. Adicionar suporte para mais sites
3. Implementar cache de resultados
4. Adicionar logging estruturado
5. Testes automatizados

## 🐛 Troubleshooting

### "Error: OPENAI_API_KEY not found"
- Verificar se `.env` existe e contém `OPENAI_API_KEY`
- Verificar se API key é válida no OpenAI dashboard

### "No properties found"
- Verificar se o site está acessível
- Verificar se estrutura HTML do site mudou
- Aumentar token limit se necessário

### Requisições lentas
- Ajustar `REQUEST_DELAY` em `config.py`
- Verificar velocidade da internet
- OpenAI rate limiting pode estar ativo

## 📄 Licença

MIT
