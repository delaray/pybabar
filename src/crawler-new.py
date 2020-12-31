# ***************************************************************************
# WEb Crawling Tools Using Scrapy Framework
# ***************************************************************************

# Scrapy Documentation: https://docs.scrapy.org/en/latest/topics/spiders.html

import scrapy as sp

class MySpider(scrapy.Spider):
    name = 'example.com'
    allowed_domains = ['example.com']
    start_urls = [
        'http://www.example.com/1.html',
        'http://www.example.com/2.html',
        'http://www.example.com/3.html',
    ]

    def parse(self, response):
        self.logger.info('A response from %s just arrived!', response.url)
        
#----------------------------------------------------------------------------
# End of File 
#----------------------------------------------------------------------------
