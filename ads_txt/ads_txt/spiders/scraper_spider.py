import datetime
import json
import re
# from urllib.parse import urlparse

import scrapy
import tldextract
from scrapy.http.response import Response
from scrapy import signals
from scrapy.spiders import Spider
from scrapy.utils.project import get_project_settings

from ads_txt.email_utils import send_email
from ads_txt.items import AdsTxtItem, AdsTxtErrorItem, AdsTxtMetadataItem


settings = get_project_settings()


class ScraperSpider(scrapy.Spider):
    name = "ads_txt_spider"

    def __init__(self, start_urls_with_meta=None, *args, **kwargs):
        super(ScraperSpider, self).__init__(*args, **kwargs)
        self.start_urls_with_meta = start_urls_with_meta or []
        self.execution_date = settings.get("EXECUTION_DATE").strftime("%Y-%m-%d")

    def start_requests(self):
        """Starts requests, prioritizing `ads_txt_page_url` if provided."""
        for domain_count, (domain, meta_data) in enumerate(self.start_urls_with_meta):
            self.logger.info(f"domain count: {domain_count}")

            inventory_type = meta_data.get("inventory_type", "ads.txt").strip()
            http_client = meta_data.get("http_client", "curl_cffi").strip()
            ads_txt_page_url = meta_data.get("ads_txt_page_url", "").strip()
            meta_data["check_root_domain"] = True
            meta_data["add_new_domain_if_found"] = True
            # self.logger.info(f"{meta_data=}")

            is_valid, message = ScraperSpider.check_if_valid_url(domain)

            if not is_valid:
                if domain is None:
                    domain = "None"
                self.logger.info(
                    f"given str is not valid domain or url: {domain} and {message=}"
                )

                yield from self.log_failure(
                    domain,
                    message,
                    "not_valid_domain_or_url",
                    "NA",
                    "NA",
                    inventory_type,
                    http_client,
                )

            else:
                meta = {
                    "original_domain": domain,
                    "attempts": 0,
                    "url_list": self.generate_ads_txt_urls(domain, inventory_type),
                    "meta_data": meta_data,
                    "inventory_type": inventory_type,
                    "http_client": http_client,
                    "execution_date": self.execution_date,
                }
                # ads_txt_page_url = meta_data.get("ads_txt_page_url", "").strip()
                # ads_txt_page_url = ads_txt_page_url.strip() if ads_txt_page_url else ""

                if ads_txt_page_url:
                    self.logger.info(
                        f"Trying provided ads_txt_page_url: {ads_txt_page_url}"
                    )
                    yield scrapy.Request(
                        url=ads_txt_page_url,
                        callback=self.parse_ads_txt,
                        errback=self.handle_error,
                        meta=meta,
                    )
                else:
                    yield scrapy.Request(
                        url=meta["url_list"][0],
                        callback=self.parse_ads_txt,
                        errback=self.handle_error,
                        meta=meta,
                    )

    def generate_ads_txt_urls(self, domain, inventory_type):
        """Generates possible `ads.txt` or `app-ads.txt` URLs for a given domain."""

        # Clean the string: strip spaces and remove non-printing characters
        domain = "".join(ch for ch in domain.strip() if ch.isprintable())

        parsed = tldextract.extract(domain)
        main_domain = f"{parsed.domain}.{parsed.suffix}"
        inventory_type = inventory_type.strip()

        urls = [
            f"https://www.{main_domain}/{inventory_type}",
            f"https://{main_domain}/{inventory_type}",
            f"http://www.{main_domain}/{inventory_type}",
            f"http://{main_domain}/{inventory_type}",
        ]

        if parsed.subdomain and parsed.subdomain.lower() != "www":
            urls.insert(
                0,
                f"https://{parsed.subdomain + '.' if parsed.subdomain else ''}{main_domain}/{inventory_type}",
            )
            urls.insert(
                1,
                f"http://{parsed.subdomain + '.' if parsed.subdomain else ''}{main_domain}/{inventory_type}",
            )

        return urls

    def parse_ads_txt(self, response: Response):
        """Validates `ads.txt`, retries if invalid."""
        ads_txt_line = response.text.strip()
        response_meta = response.meta
        original_domain = response_meta["original_domain"]

        if not self.is_valid_ads_txt(ads_txt_line):
            self.logger.info(
                f"Invalid ads.txt content at {response.url}, retrying next..."
            )
            is_domain_same = ScraperSpider.is_original_or_new_domain_same(
                original_domain, response.url
            )

            # self.logger.info(f"{old_root_domain} for {meta=}")
            new_domain_bool = response_meta["meta_data"]["add_new_domain_if_found"]

            if (not is_domain_same) and new_domain_bool:
                response_meta["meta_data"]["add_new_domain_if_found"] = False  #  only one time

                parsed = tldextract.extract(response.url)
                new_root_domain = f"{parsed.domain}.{parsed.suffix}"

                self.logger.info(
                    f"New domain changed from {original_domain} to {new_root_domain}. Retrying..."
                )

                (response_meta["url_list"]).extend(
                    self.generate_ads_txt_urls(
                        new_root_domain, response_meta["inventory_type"]
                    )
                )
            # if response_meta["attempts"] < len(response_meta["url_list"]):
            yield from self.retry_next(response_meta)
            # self.logger.warning(f"{not (response_meta["attempts"] < len(response_meta["url_list"]))=} for the url {response_meta["original_domain"]}")
            if not (response_meta["attempts"] <= len(response_meta["url_list"])):
                response_code = response.status
                error_msg = repr_error_msg = "ads_txt_page_not_found"
                inventory_type = response_meta.get("inventory_type", "NA")
                http_client = response_meta.get("http_client", "NA")
                headers_json = self.header_json_str(response)
                # self.logger.warning("testing log failure")
                yield from self.log_failure(
                    response_meta["original_domain"],
                    error_msg,
                    repr_error_msg,
                    response_code,
                    headers_json,
                    inventory_type,
                    http_client,
                )
        else:
            headers_json = self.header_json_str(response)
            self.logger.info(
                f"Ads txt data found for the url: {response_meta['original_domain']}"
            )

            yield AdsTxtItem(
                domain=response_meta["original_domain"],
                ads_txt_url=response.url,
                ads_txt_line=ads_txt_line,
                inventory_type=response_meta["inventory_type"],
                execution_date=self.execution_date,
            )

            yield AdsTxtMetadataItem(
                original_domain=response_meta["original_domain"],
                ads_page_url=response.url,
                response_header_json_str=headers_json,
                inventory_type=response_meta["inventory_type"],
                http_client=response_meta["http_client"],
                execution_date=self.execution_date,
            )

    def is_valid_ads_txt(self, content: str):
        content_str = content.replace(" ", "").lower()

        return (
            "google.com,pub-" in content_str
            or "inventorypartnerdomain=" in content_str
            or "ownerdomain=" in content_str
            or "xandr.com," in content_str
            or "pubmatic.com," in content_str
            or "adform.com," in content_str
            or "appnexus.com," in content_str
            or "improvedigital.com," in content_str
            or "netlink.vn," in content_str
            or "rubiconproject.com," in content_str
            or "freewheel.tv," in content_str
            or "33across.com," in content_str
            or "aps.amazon.com," in content_str
            or "contextweb.com," in content_str
            or "conversantmedia.com," in content_str
            or "indexexchange.com," in content_str
            or "adingo.jp," in content_str
            or "facebook.com," in content_str
            or "video.unrulymedia.com," in content_str
            or "admixer.net," in content_str
            or "applovin.com," in content_str
            or "hoppex.hu," in content_str
            or "adswizz.com," in content_str
            or "applovin.com," in content_str
            or "applovin.com," in content_str
        )

    def handle_error(self, failure):
        """Handles request failures and retries the next URL."""
        request = failure.request

        meta = request.meta
        original_domain = meta["original_domain"]
        inventory_type = meta.get("inventory_type", "NA")
        http_client = meta.get("http_client", "NA")

        error_msg = str(failure)
        repr_error_msg = repr(failure)

        try:
            response = failure.value.response if failure.value else "NA"
            response_code = response.status if response else "NA"
            headers_json = self.header_json_str(response)
        except Exception as e:
            self.logger.warning(f"Got a fatal error: {original_domain} as {e}")
            response_code = "NA"
            headers_json = "NA"

        return (
            self.retry_next(meta)
            if meta["attempts"] <= len(meta["url_list"])
            else self.log_failure(
                original_domain,
                error_msg,
                repr_error_msg,
                response_code,
                headers_json,
                inventory_type,
                http_client,
            )
        )

    def retry_next(self, meta):
        """Retries the next URL in the list if available, or checks if the root domain has changed."""
        attempts = meta["attempts"]
        url_list = meta["url_list"]

        if attempts < len(url_list):
            next_url = url_list[attempts]
            self.logger.info(f"Retrying with: {next_url}")
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_ads_txt,
                errback=self.handle_error,
                meta={**meta, "attempts": attempts + 1},
            )
        else:
            # All attempts failed, check if the root domain has changed
            original_domain = meta["original_domain"]
            self.logger.info(
                f"All attempts failed for {original_domain}. Checking root domain..."
            )

            root_domain_url = f"https://{original_domain}"
            yield scrapy.Request(
                url=root_domain_url,
                callback=self.check_root_domain,
                errback=self.handle_error,
                meta={**meta, "attempts": attempts + 1},
            )

    def check_root_domain(self, response: Response):
        """Checks if the root domain has changed and retries with the new domain if applicable."""
        meta = response.meta
        original_domain = meta["original_domain"]

        is_domain_same = ScraperSpider.is_original_or_new_domain_same(
            original_domain, response.url
        )

        # self.logger.info(f"{old_root_domain} for {meta=}")
        check_root_domain_bool = meta["meta_data"]["check_root_domain"]

        if (not is_domain_same) and check_root_domain_bool:
            meta["meta_data"]["check_root_domain"] = False
            # self.logger.debug(f"{meta["check_root_domain"]=}")
            parsed = tldextract.extract(response.url)
            new_root_domain = f"{parsed.domain}.{parsed.suffix}"

            self.logger.info(
                f"Root domain changed from {original_domain} to {new_root_domain}. Retrying..."
            )
            # meta["original_domain"] = new_root_domain
            meta["url_list"] = self.generate_ads_txt_urls(
                new_root_domain, meta["inventory_type"]
            )
            meta["attempts"] = 0  # Reset attempts for the new domain

            # Start the process again with the new root domain
            yield scrapy.Request(
                url=meta["url_list"][0],
                callback=self.parse_ads_txt,
                errback=self.handle_error,
                meta=meta,
            )
        else:
            self.logger.info(
                f"Root domain has not changed for {original_domain}. Logging failure..."
            )
            yield from self.log_failure(
                original_domain,
                "all_attempts_failed",
                "all_attempts_failed",
                response.status,
                self.header_json_str(response),
                meta.get("inventory_type", "NA"),
                meta.get("http_client", "NA"),
            )

    def log_failure(
        self,
        domain,
        error_msg,
        repr_error_msg,
        response_code,
        headers_json,
        inventory_type,
        http_client,
    ):
        """Logs failure and saves error details."""
        self.logger.warning(f"Failed to find {inventory_type} for {domain}")
        yield AdsTxtErrorItem(
            domain=domain,
            error_msg=error_msg,
            repr_error_msg=repr_error_msg,
            response_code=str(response_code),
            response_header_json_str=headers_json,
            inventory_type=inventory_type,
            http_client=http_client,
            execution_date=self.execution_date,
        )

    def header_json_str(self, response: Response):
        try:
            if hasattr(response.headers, "to_unicode_dict"):
                headers_json = json.dumps(dict(response.headers.to_unicode_dict()))
            else:
                headers_json = json.dumps(dict(response.headers))
        except Exception as e:
            self.logger.warning(f"header_json: {e}")
            self.logger.warning(response.meta)
            headers_json = ""

        return headers_json

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(ScraperSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_opened(self):
        if settings.get("SEND_MAIL_BOOL"):
            send_email("Scrapy Started", "Your Scrapy spider has started running.")

    def spider_closed(self, spider: Spider, reason):
        """Save final Scrapy stats to a JSON file on shutdown."""
        stats = spider.crawler.stats.get_stats()

        self.stats_to_gb(stats)

        # Format stats into a readable message
        stats_message = "\n".join([f"{key}: {value}" for key, value in stats.items()])

        # Email subject and body
        subject = "Scrapy Finished - " + reason
        body = f"Your Scrapy spider has stopped.\nReason: {reason}\n\nStats:\n{stats_message}"

        if settings.get("SEND_MAIL_BOOL"):
            send_email(subject, body)

    @staticmethod
    def stats_to_gb(stats):
        # Extract request (upload) and response (download) sizes
        total_upload_bytes = stats.get("downloader/request_bytes", 0)
        total_download_bytes = stats.get("downloader/response_bytes", 0)

        # Convert bytes to GB (1 GB = 1,073,741,824 bytes)
        total_upload_gb = total_upload_bytes / 1_073_741_824
        total_download_gb = total_download_bytes / 1_073_741_824
        total_usage_gb = total_upload_gb + total_download_gb

        # Print the results
        print("\n📊 Scrapy Bandwidth Usage 📊")
        print(f"🔼 Upload: {total_upload_gb:.4f} GB")
        print(f"🔽 Download: {total_download_gb:.4f} GB")
        print(f"🌍 Total Usage: {total_usage_gb:.4f} GB\n")

        stats["total_upload_gb"] = f"{total_upload_gb:.4f} GB"
        stats["total_download_gb"] = f"{total_download_gb:.4f} GB"
        stats["total_usage_gb"] = f"{total_usage_gb:.4f} GB"

        # Given start time as a string
        start_time = stats.get("start_time", 0)

        start_time_str = start_time.strftime("%Y-%m-%d %H:%M:%S.%f%z")

        # Get the current time in UTC
        finish_time = datetime.datetime.now(datetime.timezone.utc)

        # Calculate the time difference
        time_passed = finish_time - start_time

        # Format finish time to match the input format
        finish_time_str = finish_time.strftime("%Y-%m-%d %H:%M:%S.%f%z")

        # Format time passed in HH:MM:SS format
        hours, remainder = divmod(time_passed.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        time_passed_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        stats["finish_time_str"] = finish_time_str
        stats["time_passed"] = time_passed_str

        # Print the results
        print("Start Time:", start_time_str)
        print("Finish Time:", finish_time_str)
        print("Time Passed (HH:MM:SS):", time_passed_str)

    @staticmethod
    def check_if_valid_url(input_string):
        # Clean the string: strip spaces and remove non-printing characters
        cleaned = "".join(ch for ch in input_string.strip() if ch.isprintable())

        # If the cleaned string is empty
        if not cleaned:
            return (False, "Invalid (Empty or Non-printing)")

        # Regex to match URLs
        url_pattern = re.compile(
            r"^(https?://)?"  # optional scheme
            r"(www\.)?"  # optional www
            r"([a-zA-Z0-9-]+\.)+"  # domain/subdomain
            r"[a-zA-Z]{2,}"  # TLD like .com, .in, etc.
            r"(/[\w\-./]*)*$",  # optional path
            re.IGNORECASE,
        )

        if url_pattern.match(cleaned):
            return (True, "Valid URL")
        else:
            return (False, "Not a URL")

    @staticmethod
    def is_original_or_new_domain_same(original_domain, new_domain):
        # Clean the string: strip spaces and remove non-printing characters
        original_domain = "".join(
            ch for ch in original_domain.strip() if ch.isprintable()
        )
        # Extract the new root domain from the response URL
        parsed = tldextract.extract(original_domain)
        old_root_domain = f"{parsed.domain}.{parsed.suffix}"
        # Extract the new root domain from the response URL
        parsed = tldextract.extract(new_domain)
        new_root_domain = f"{parsed.domain}.{parsed.suffix}"

        return old_root_domain == new_root_domain
