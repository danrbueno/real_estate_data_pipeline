import scrapy
import datetime
from unidecode import unidecode
import sys
import os
from pathlib import Path

if __name__ == "DFImoveis":
    from scraping.items import ScrapingItem
else:
    parent_dir = str(Path(os.path.dirname(__file__)).parent)
    sys.path.append(parent_dir)
    from items import ScrapingItem

# The JSON file path with the scrap result is set in pipeline.py
class DfimoveisSpider(scrapy.Spider):
    name = "DFImoveis"
    allowed_domains = ["www.dfimoveis.com.br"]
    url = "https://www.dfimoveis.com.br/{}/df/todos/apartamento?pagina={}"

    def start_requests(self):
        # Category defined at parameter -a in runspider command
        # Default is "sale"

        if(self.category == "sales"):
            self.url_category = "venda"
        elif(self.category == "rentals"):
            self.url_category = "aluguel"

        yield scrapy.Request(self.url.format(self.url_category, 1), self.parse)

    # Scrap all pages
    def parse(self, response):
        css_links = "#resultadoDaBuscaDeImoveis a"

        # Access and scrap each link in the page
        for property in response.css(css_links):
            link = "https://www.dfimoveis.com.br" + property.css("::attr(href)").extract_first()
            yield scrapy.Request(link, self.parse_property)

        # - Get the current and next page
        # - Get the total of properties and quantity of properties in each page
        # - Get the quantity of pages in pagination
        # - Start the loop to scrap other pages in pagination:
        
        current_page = response.url.split("=")[1]
        current_page = int(current_page)
        next_page = current_page + 1
        
        css_qtd_properties = "#hidden-quantidade-de-imoveis-encontrados ::attr(value)"
        qtd_properties = response.css(css_qtd_properties).extract_first()
        qtd_properties = int(qtd_properties)

        css_qtd_properties_page = "#hidden-quantidade-de-imoveis-por-pagina ::attr(value)"
        qtd_properties_page = response.css(css_qtd_properties_page).extract_first()            
        qtd_properties_page = int(qtd_properties_page)

        last_page = int(qtd_properties/qtd_properties_page)+1        

        if next_page <= last_page:
            yield scrapy.Request(self.url.format(self.url_category, next_page), self.parse)

    # Scrap all the properties links
    def parse_property(self, response):
        data = {}
        data["dt_scraping"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["link"] = response.url
        data["title"] = response.css("h1::text").extract_first().strip()        
        css_features = ".r-mobile-dados h6"
        css_details = ".col-md-12.bg-white.shadow.mt-2.pb-2 h6"

        # Scrap features identified under the elements "<h6>":
        # The key names are the <h6> text
        # The values are the identified within <small>
        # If there's no <small> under <h6>, ignore
        # Remove special chars from key name and turn it to lower case
        # If the key name is an empty text, put 'key_{x}' for key name
        x = 0
        for feature in response.css(css_features):

            value = feature.css("small::text").extract_first()
            
            if(value is not None):
                key = feature.css("::text").extract_first().strip()
                key = key.lower()
                key = key.replace(" ","_")
                key = key.replace(":","")
                key = key.replace("/","")
                key = key.replace("$","s")
                key = unidecode(key)
                
                if key == '':
                    x+=1
                    key = "key_{}".format(x)

                data[key] = value.strip()

        # Scrap details identified under the elements "<h6>":
        # The key names are the <h6> text
        # The values are the identified within <small>
        # If there's no <small> under <h6>, ignore
        # Remove special chars from key name and turn it to lower case
        # If the key name is an empty text, put 'key_{x}' for key name
        for detail in response.css(css_details):            
            value = detail.css("small::text").extract_first()
            
            if(value is not None):
                key = detail.css("::text").extract_first().strip()
                key = key.lower()
                key = key.replace(" ","_")
                key = key.replace(":","")
                key = key.replace("/","")
                key = key.replace("$","s")
                key = unidecode(key)
                
                if key == '':
                    x+=1
                    key = "key_{}".format(x)
                
                data[key] = value.strip()

        property = ScrapingItem(data = data)
        yield property
