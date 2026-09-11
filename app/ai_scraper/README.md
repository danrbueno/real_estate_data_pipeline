# AI Scraper - Web Scraping para DFImoveis

Pipeline de coleta de imóveis do site DFImoveis, dividido em duas etapas
determinísticas (sem chamadas à OpenAI) e reservando o agente de IA (`ai_agent.py`)
para uma etapa futura de extração estruturada dos campos de cada anúncio.

## ✨ Por que HTTP + regex em vez de IA para o crawling?

- **Determinístico e completo**: baixar o HTML real e extrair links com regex garante
  que TODOS os anúncios da página sejam encontrados, sem risco de alucinação
- **Sem custo de tokens**: nenhuma chamada à OpenAI é feita para navegar páginas ou
  contar anúncios
- **Simples de depurar**: falhas de rede/parsing são explícitas, ao contrário de uma
  IA que pode "inventar" ou perder resultados silenciosamente
- **IA reservada para o que ela faz bem**: extrair campos semiestruturados de uma
  página de detalhe (preço, área, quartos etc.), não para enumerar links

## 🔧 Instalação

### 1. Instalar Dependências

```bash
pip install -r config/requirements.txt
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
python -m app.ai_scraper.ad_links_collector.main --type rentals

# Scrape sales
python -m app.ai_scraper.ad_links_collector.main --type sales
```

### Em Código Python

```python
from ai_scraper import AdLinksCollector

scraper = AdLinksCollector()
pages = scraper.collect("rentals")
scraper.close()

total_links = sum(len(page["links"]) for page in pages)
print(f"Extracted {total_links} ad links across {len(pages)} pages")
```

O primeiro agente (`ad_links_collector`) baixa o HTML de cada página de listagem
via `HTTPClient` (sem IA) e extrai, via regex, os links dos anúncios (sem salvar o
HTML). O resultado é gravado em `data/raw/<tipo>/links.json`, no formato:

```json
[
  {"page": 1, "links": ["https://www.dfimoveis.com.br/imovel/...", "..."]}
]
```

### Download dos detalhes dos anúncios

O segundo agente lê os links salvos, baixa cada página de detalhe (também via
`HTTPClient`, sem IA) e salva o HTML em `data/raw/<tipo>/properties/`.

```bash
python -m app.ai_scraper.property_pages_downloader.main --type rentals
python -m app.ai_scraper.property_pages_downloader.main --type sales
```

### Com Airflow (DAG)

```python
from ai_scraper import AdLinksCollector

def scrape_rentals():
    scraper = AdLinksCollector()
    pages = scraper.collect("rentals")
    scraper.close()
    return sum(len(page["links"]) for page in pages)

def scrape_sales():
    scraper = AdLinksCollector()
    pages = scraper.collect("sales")
    scraper.close()
    return sum(len(page["links"]) for page in pages)
```

## 📊 Estrutura de Saída

- `data/raw/<tipo>/links.json` — links de anúncios por página (ver formato acima)
- `data/raw/<tipo>/properties/*.html` — HTML bruto de cada página de anúncio,
  pronto para uma etapa futura de extração de campos

A extração de campos estruturados (preço, quartos, área etc.) a partir do HTML de
cada anúncio ainda depende do agente de IA (`AIScrapingAgent.extract_property_page_details`
em `ai_agent.py`), mas essa etapa ainda não está conectada a um script de linha de
comando — é usada apenas diretamente via código/testes por enquanto.

## 🤖 Como Funciona

1. **Fetch da Página**: `ad_links_collector` baixa o HTML da página de listagem
   via HTTP puro (sem salvar o arquivo, sem chamar a OpenAI)
2. **Extração de Links**: Um regex extrai os links dos anúncios diretamente do HTML
3. **Paginação**: Navega para a próxima página até encontrar uma sem anúncios
4. **Detalhes do Imóvel**: `property_pages_downloader` baixa o HTML de cada anúncio
   (via HTTP puro) e salva o arquivo localmente para processamento posterior
5. **Salvamento**: Os links de cada página ficam em `data/raw/<tipo>/links.json` e as
   páginas dos anúncios em `data/raw/<tipo>/properties/`

## 📝 Arquitetura

```
ai_scraper/
├── __init__.py                       # Package initialization
├── config.py                         # Configuration and constants
├── http_client.py                    # HTTP requests with rate limiting
├── ai_agent.py                       # OpenAI agent (reserved for detail-field extraction)
├── ad_links_collector/
│   ├── ad_links_collector.py         # Fetch listing pages + extract ad links (regex)
│   └── main.py                       # CLI entry point
└── property_pages_downloader/
    ├── property_pages_downloader.py  # Download each ad's detail page HTML
    └── main.py                       # CLI entry point
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
```

## 🔒 Segurança

- API key da OpenAI (quando usada para extração de campos) armazenada em `.env`
  (nunca commitado)
- Rate limiting automático nas requisições HTTP
- User-Agent configurado
- Tratamento de erros robusto

## 💡 Próximos Passos

1. Conectar `AIScrapingAgent.extract_property_page_details` a um script de linha de
   comando que leia `data/raw/<tipo>/properties/*.html` e grave os campos extraídos
2. Integrar com DAG do Airflow
3. Adicionar suporte para mais sites
4. Implementar cache de resultados
5. Adicionar logging estruturado

## 🐛 Troubleshooting

### "Error: OPENAI_API_KEY not found"
- Só é necessário se você for usar `AIScrapingAgent` para extrair campos de uma
  página de detalhe; não afeta `ad_links_collector` nem `property_pages_downloader`
- Verificar se `.env` existe e contém `OPENAI_API_KEY`

### "Page X has NO properties" muito cedo
- Verificar se o site está acessível
- Verificar se a estrutura HTML do site mudou (o regex espera hrefs `/imovel/...`)

### Requisições lentas
- Ajustar `REQUEST_DELAY` em `config.py`
- Verificar velocidade da internet

## 📄 Licença

MIT
