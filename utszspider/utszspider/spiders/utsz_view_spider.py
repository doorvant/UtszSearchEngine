import scrapy
import re
import requests
import pickle
from items import NewsItem
from urllib.parse import urljoin


class UtszViewSpider(scrapy.Spider):
    # 用于区别Spider
    name = "UtszViewSpider"
    # 允许访问的域
    allowed_domains = ['utsz.edu.cn']
    # 爬取的起始地址
    start_urls = ['https://www.utsz.edu.cn/index.html']
    # 将要爬取的地址列表
    destination_list = start_urls
    # 已爬取地址md5集合
    url_md5_seen = []
    # 断点续爬计数器
    counter = 0
    # 保存频率，每多少次爬取保存一次断点
    save_frequency = 50

    def __init__(self):
        super
        # 读取已保存断点
        import os
        if not os.path.exists('./pause/'):
            os.mkdir('./pause/')
        if not os.path.isfile('./pause/response.seen'):
            f = open('./pause/response.seen', 'wb')
            f.close()
        if not os.path.isfile('./pause/response.dest'):
            f = open('./pause/response.dest', 'wb')
            f.close()

        f = open('./pause/response.seen', 'rb')
        if os.path.getsize('./pause/response.seen') > 0:
            self.url_md5_seen = pickle.load(f)
        f.close()
        f = open('./pause/response.dest', 'rb')
        if os.path.getsize('./pause/response.dest') > 0:
            self.start_urls = pickle.load(f)
            self.destination_list = self.start_urls
        f.close()
        self.counter += 1

    # 爬取方法
    def parse(self, response):

        # 保存断点
        self.counter_plus()

        # 爬取当前网页
        print('start parse : ' + response.url)
        self.destination_list.remove(response.url)
        if response.url.startswith("https://www.utsz.edu.cn/article/view/"):
            item = NewsItem()
            for box in response.xpath('//section[@class="wrapper"]/section[@class="container"]'):
                # 文章标题
                item['newsTitle'] = box.xpath('.//div[@class="leftscroll"]/header/h3/text()').extract()[0].strip()

                # 网页链接
                item['newsUrl'] = response.url
                item['newsUrlMd5'] = self.md5(response.url)

                # 发布时间
                item['newsPublishTime'] = box.xpath('.//div[@class="leftscroll"]/header/span[@class="date"]/text()').extract()[0].strip()

                # 文章内容
                item['newsContent'] = box.xpath('.//div[@class="edittext"]').extract()[0].strip()
                regexp = re.compile(r'<[^>]+>', re.S)
                item['newsContent'] = regexp.sub('',item['newsContent'])    # delete templates <>

                # 索引构建flag
                item['indexed'] = 'False'

                # yield it
                yield item

        # 获取当前网页所有url并进行广度优先度爬取
        urls = response.xpath('//a/@href').extract()
        for url in urls:
            real_url = urljoin(response.url, url)   # 将.//等简化url转化为真正的http格式url
            if not real_url.startswith('https://www.utsz.edu.cn'):
                continue    # 保持爬虫在utsz.edu.cn之内
            if real_url.endswith('.jpg') or real_url.endswith('.pdf'):
                continue    # 图片资源不爬
            if '.jsp?' in real_url:
                continue    # 动态网站不爬
            # md5 检查
            md5_url = self.md5(real_url)
            if self.binary_md5_url_search(md5_url) > -1:    # 存在当前MD5
                pass
            else:
                self.binary_md5_url_insert(md5_url)
                self.destination_list.append(real_url)
                yield scrapy.Request(real_url, callback=self.parse, errback=self.errback_httpbin)

    def md5(self, val):
        import hashlib
        ha = hashlib.md5()
        ha.update(bytes(val, encoding='utf-8'))
        key = ha.hexdigest()
        return key

    # 断点计数器加一，并在合适的时候保存断点
    def counter_plus(self):
        print('待爬取网址数：' + (str)(len(self.destination_list)))
        # 断点续爬功能之保存断点
        if self.counter % self.save_frequency == 0:  # save_frequency：爬取次数
            print('正在保存爬虫断点....')

            f = open('./pause/response.seen', 'wb')
            pickle.dump(self.url_md5_seen, f)
            f.close()

            f = open('./pause/response.dest', 'wb')
            pickle.dump(self.destination_list, f)
            f.close()

            self.counter = self.save_frequency

        self.counter += 1  # 计数器加一

    # scrapy.request请求失败后的处理
    def errback_httpbin(self, failure):
        self.destination_list.remove(failure.request._url)
        print('Error 404 url deleted: ' + failure.request._url)
        self.counter_plus()

    # 二分法md5集合排序插入self.url_md5_set--16进制md5字符串集
    def binary_md5_url_insert(self, md5_item):
        low = 0
        high = len(self.url_md5_seen)
        while (low < high):
            mid = (int)(low + (high - low) / 2)
            if self.url_md5_seen[mid] < md5_item:
                low = mid + 1
            elif self.url_md5_seen[mid] >= md5_item:
                high = mid
        self.url_md5_seen.insert(low, md5_item)

    # 二分法查找url_md5存在于self.url_md5_set的位置，不存在时返回-1
    def binary_md5_url_search(self, md5_item):
        low = 0
        high = len(self.url_md5_seen)
        if high == 0:
            return -1
        while (low < high):
            mid = (int)(low + (high - low) / 2)
            if self.url_md5_seen[mid] < md5_item:
                low = mid + 1
            elif self.url_md5_seen[mid] > md5_item:
                high = mid
            elif self.url_md5_seen[mid] == md5_item:
                return mid
        if low >= self.url_md5_seen.__len__():
            return -1
        if self.url_md5_seen[low] == md5_item:
            return low
        else:
            return -1