from scraping import settings as my_settings
from scraping.spiders.DFImoveis import DfimoveisSpider
from scraping.spiders.Olx import OlxSpider
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings


def main():
    crawler_settings = Settings()
    crawler_settings.setmodule(my_settings)
    process = CrawlerProcess(settings=crawler_settings)
    
    process.crawl(DfimoveisSpider, input="inputargument", category="rental")
    process.crawl(DfimoveisSpider, input="inputargument", category="sale")
    
    process.start()

if __name__ == "__main__":
    main()
