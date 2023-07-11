import datetime
import json
import os
import sys
from pathlib import Path

import scrapy
from scrapy import Selector
from scrapy_splash import SplashRequest
from unidecode import unidecode

if __name__ == "Olx":
    from scraping.items import ScrapingItem
else:
    parent_dir = str(Path(os.path.dirname(__file__)).parent)
    sys.path.append(parent_dir)
    from items import ScrapingItem

LUA_SCRIPT = """
function main(splash, args)
  splash.private_mode_enabled = false
  assert(splash:go(args.url))
  assert(splash:wait(20))
  return {
    html = splash:html(),
    png = splash:png(),
    har = splash:har(),
  }
end
"""


# The JSON file path with the scrap result is set in pipeline.py
class OlxSpider(scrapy.Spider):
    name = "Olx"
    allowed_domains = []
    start_urls = ["https://www.olx.com.br/imoveis/{}/apartamentos/estado-df"]

    def start_requests(self):

        # Category defined at parameter -a in runspider command
        # Default is "sale"

        if self.category == "sale":
            self.url_category = "venda"
        elif self.category == "rental":
            self.url_category = "aluguel"

        for url in self.start_urls:
            yield SplashRequest(
                url.format(self.url_category),
                self.parse,
                endpoint="execute",
                args={"lua_source": LUA_SCRIPT},
            )

    # Scrap all pages
    def parse(self, response):
        selector = Selector(response)
        links = selector.xpath("//a[contains(@data-ds-component, 'DS-NewAdCard-Link')]")        

        # Access and scrap each link in the page
        for link in links:
            url = link.css("::attr(href)").extract_first()
            yield SplashRequest(
                url=url,
                callback=self.parse_property,
                endpoint="execute",
                args={"lua_source": LUA_SCRIPT},
            )

        # Pagination
        next_page = selector.xpath("//a[contains(@data-lurker-detail, 'next_page')]")

        if len(next_page) >= 1:
            url = next_page.css("::attr(href)").extract_first()
            yield SplashRequest(
                url=url,
                callback=self.parse,
                endpoint="execute",
                args={"lua_source": LUA_SCRIPT},
            )

    # Scrap all the properties links
    def parse_property(self, response):
        selector = Selector(response)
        data = {}
        main_div = "/html/body/div[2]/div/div[4]/div[2]/div/div[2]/div[1]"
        data["dt_scraping"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["link"] = response.url
        data["title"] = selector.xpath(main_div+"/div[6]/div/div/h1/text()").get()
        data["price"] = selector.xpath(main_div+"/div[3]/div/div/div[2]/div/div[1]/div/div[1]/span/text()").get()

        ad_properties = selector.xpath("//div[contains(@data-testid, 'ad-properties')]").getall()

        for ad_property in ad_properties:
            # print(ad_property)
            ad_selector = Selector(text=ad_property)
            details = ad_selector.xpath("//div[contains(@data-ds-component, 'DS-Flex')]").getall()

            for text in details:
                text_selector = Selector(text=text)
                ds_text = text_selector.xpath("//span[contains(@data-ds-component, 'DS-Text')]/text()").getall()
                
                if(len(ds_text)>=2):
                    key = ds_text[0].strip()
                    key = key.lower()
                    key = key.replace(" ","_")
                    key = key.replace(":","")
                    key = key.replace("/","")
                    key = key.replace("$","s")
                    key = unidecode(key)

                    data[key] = ds_text[1]
                
        property = ScrapingItem(data=data)
        yield property