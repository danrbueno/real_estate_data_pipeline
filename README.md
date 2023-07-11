# real_estate_scraping
Get information from apartments rental or sales from real estate ads websites

Commands for terminal to start project:
cd imoveis_bsb\scripts
scrapy startproject scraping
cd imoveis_bsb\scripts\scraping

For "DF Imóveis":
scrapy genspider DFImoveis https://www.dfimoveis.com.br/aluguel/df/todos/apartamento
scrapy shell https://www.dfimoveis.com.br/aluguel/df/todos/apartamento
scrapy runspider scraping/spiders/DFImoveis.py -a category=rental
scrapy shell https://www.dfimoveis.com.br/venda/df/todos/apartamento
scrapy runspider scraping/spiders/DFImoveis.py -a category=sale
