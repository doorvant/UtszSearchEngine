# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class NewsItem(scrapy.Item):
    newsTitle = scrapy.Field()
    newsUrl = scrapy.Field()
    newsUrlMd5 = scrapy.Field()
    newsPublishTime = scrapy.Field()
    newsContent = scrapy.Field()
    indexed = scrapy.Field()    #
