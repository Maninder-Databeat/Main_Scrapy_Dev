# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.spiders import Spider

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter


class AdsTxtSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class AdsTxtDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


# import certifi
# import truststore
# import ssl
from curl_cffi.requests import AsyncSession as curl_async_session, errors
from curl_cffi import requests
from scrapy.http import HtmlResponse
from scrapy.http.request import Request

# from .custom_utils import DEFAULT_REQUEST_HEADERS

# Load both certifi and truststore
# CUSTOM_SSL_CONTEXT = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)  # Uses system certificates
# CUSTOM_SSL_CONTEXT.load_verify_locations(certifi.where())  # Add Mozilla certs


class DynamicHttpClientMiddleware:
    """Middleware to dynamically choose HTTP client based on request metadata."""

    async def process_request(self, request: Request, spider):
        """Check request metadata and decide which HTTP client to use."""

        http_client = request.meta.get("http_client", "default")  # Default is Scrapy

        if http_client == "curl_cffi":
            return await self.use_curl_cffi(request, spider)
        return None  # Use Scrapy's default downloader

    async def use_curl_cffi(self, request: Request, spider: Spider):
        """Use curl-cffi for the request."""
        spider.logger.info(f"Using curl-cffi for {request.url}")
        try:
            try:
                # spider.logger.info(f"Using curl-cffi for {request.url} - 2")
                async with curl_async_session() as client:
                    # spider.logger.info(f"Using curl-cffi for {request.url} - 3")
                    response = await client.get(
                        request.url,
                        timeout=10,
                        impersonate="chrome",
                        allow_redirects=True,
                        verify=True,
                        max_redirects=5,
                    )
                    request.meta["ssl_verify"] = True
                    # spider.logger.info(f"Using curl-cffi for {request.url} - 4")
                    # response.e

            except errors.CurlError as e:
                spider.logger.warning(
                    f"SSL Error or other exception for {request.url}: {e}. Retrying with custom SSL context."
                )
                async with curl_async_session() as client:
                    response = await client.get(
                        request.url,
                        timeout=10,
                        impersonate="chrome",
                        allow_redirects=True,
                        verify=False,
                        max_redirects=5,
                    )
                    request.meta["ssl_verify"] = False

            # (Removed content-encoding because if passed in html response it will raise error due to double compression)
            headers_dict = {
                k: [v]
                for k, v in response.headers.items()
                if k.lower() != "content-encoding"
            }  # Converts curl_cffi headers

            return HtmlResponse(
                url=response.url,
                body=response.content,
                request=request,
                status=response.status_code,
                headers=headers_dict,
            )
        except errors.CurlError as e:
            spider.logger.error(f"curl-cffi request failed for {request.url}: {e}")
        except Exception as e:
            spider.logger.error(f"The request failed for {request.url}: {e}")
        return None  # Fall back to Scrapy
