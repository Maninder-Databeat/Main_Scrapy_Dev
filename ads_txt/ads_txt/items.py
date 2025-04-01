# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AdsTxtItem(scrapy.Item):
    domain = scrapy.Field()
    ads_txt_url = scrapy.Field()
    ads_txt_line = scrapy.Field()
    inventory_type = scrapy.Field()  # ads.txt or app-ads.txt
    execution_date = scrapy.Field()

class AdsTxtErrorItem(scrapy.Item):
    domain = scrapy.Field()
    error_msg = scrapy.Field()
    repr_error_msg = scrapy.Field()
    response_code = scrapy.Field()
    response_header_json_str = scrapy.Field()
    inventory_type = scrapy.Field()  # ads.txt or app-ads.txt
    http_client = scrapy.Field()
    execution_date = scrapy.Field()

class AdsTxtMetadataItem(scrapy.Item):
    original_domain = scrapy.Field()
    ads_page_url = scrapy.Field()
    response_header_json_str = scrapy.Field()
    inventory_type = scrapy.Field()  # ads.txt or app-ads.txt
    http_client = scrapy.Field()
    execution_date = scrapy.Field()