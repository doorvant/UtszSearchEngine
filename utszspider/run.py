from scrapy import cmdline

cmdline.execute("scrapy crawl UtszViewSpider".split())
# cmdline.execute("scrapy crawl UtszViewSpider -s JOBDIR=crawls/somespider-1".split())