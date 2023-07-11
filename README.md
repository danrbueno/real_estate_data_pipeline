# real_estate_scraping
Scrap data from Brasília, Brazil real estate rental and sales websites. 
Websites included in this project:
- https://www.dfimoveis.com.br

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
