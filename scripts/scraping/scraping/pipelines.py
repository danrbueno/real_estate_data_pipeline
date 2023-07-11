# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
import os
from pathlib import Path

class ScrapingPipeline:
    def open_spider(self, spider):
        parent_dir = str(Path(os.path.dirname(__file__)).parent.parent.parent)
        file_path = "{}\\datasets\\{}\\{}.json".format(parent_dir, spider.name, spider.category)
        self.file = open(file_path, "w")

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item
