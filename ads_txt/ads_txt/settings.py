# # Scrapy settings for ads_txt project
# #
# # For simplicity, this file contains only settings considered important or
# # commonly used. You can find more settings consulting the documentation:
# #
# #     https://docs.scrapy.org/en/latest/topics/settings.html
# #     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# #     https://docs.scrapy.org/en/latest/topics/spider-middleware.html


# import datetime
# from pathlib import Path
# import os


# BOT_NAME = "ads_txt"

# SPIDER_MODULES = ["ads_txt.spiders"]
# NEWSPIDER_MODULE = "ads_txt.spiders"

# EXECUTION_DATE = datetime.datetime.now(datetime.timezone.utc)
# CURRENT_WORKING_DIR = Path.cwd()
# utc_date_str = EXECUTION_DATE.strftime("%Y-%m-%d")
# utc_datetime_L1D_str = (EXECUTION_DATE - datetime.timedelta(days=1)).strftime(
#     "%Y-%m-%d"
# )
# file_suffix = EXECUTION_DATE.strftime("%Y-%m-%d-%H-%M-%S")

# # Custom settings
# CUSTOM_RUN_SETTINGS = {
#     "COMMON": {
#         "EXECUTION_DATE": file_suffix,
#         "BATCH_SIZE": 100,
#         "ROOT_DATA_OUTPUT": CURRENT_WORKING_DIR / "data_output" / utc_date_str,
#         "ROOT_DATA_OUTPUT_L1D": CURRENT_WORKING_DIR
#         / "data_output"
#         / utc_datetime_L1D_str,
#         "LOG_FILE_PATH": CURRENT_WORKING_DIR
#         / "scrapy_logs"
#         / f"scrapy_log_{file_suffix}.log",
#     },
#     "CLOUD_SAVE": {
#         "SERVICE_ACCOUNT_JSON_PATH": CURRENT_WORKING_DIR
#         / ".secrets"
#         # / os.getenv("SERVICE_ACCOUNT_FILE_NAME"),
#         / (os.getenv("SERVICE_ACCOUNT_FILE_NAME") or "dummy_sa.json"),
#         "PROJECT_ID": "ads-txt-validator",
#         "TO_GCS": {
#             "UPLOAD_TO_GCS": False,
#             "GCS_BUCKET": os.getenv("GCS_BUCKET"),
#             "GCS_SUCCESS_GRI": f"bronze/ads_txt_success/{utc_date_str}/ads_txt_success_{file_suffix}.parquet",
#             "GCS_FAILURE_GRI": f"bronze/ads_txt_failure/{utc_date_str}/ads_txt_failure_{file_suffix}.parquet",
#             "GCS_METADATA_GRI": f"bronze/ads_txt_metadata/{utc_date_str}/ads_txt_metadata_{file_suffix}.parquet",
#         },
#         "TO_BQ": {
#             "UPLOAD_GCS_TO_BQ": False,
#             "BQ_DATASET_ID": os.getenv("BQ_DATASET_ID"),
#             "ADS_TXT_SUCCESS_BQ_TABLE_ID": "bronze_all_domains_today",
#             "ADS_TXT_FAILURE_BQ_TABLE_ID": "domain_failure_data_today",
#             "ADS_TXT_METADATA_BQ_TABLE_ID": "domain_meta_data",
#         },
#     },
# }

# # Common settings
# CUSTOM_RUN_SETTINGS["COMMON"]["REMOVE_SAME_DAY_OUTPUT_FILES"] = False
# CUSTOM_RUN_SETTINGS["COMMON"][
#     "REMOVE_YESTERDAY_OUTPUT_FILES"
# ] = not CUSTOM_RUN_SETTINGS["COMMON"]["REMOVE_SAME_DAY_OUTPUT_FILES"]


# CUSTOM_RUN_SETTINGS["COMMON"]["LOCAL_SUCCESS_FILE_PATH"] = (
#     CUSTOM_RUN_SETTINGS["COMMON"]["ROOT_DATA_OUTPUT"]
#     / f"ads_txt_success_{file_suffix}.parquet"
# )
# CUSTOM_RUN_SETTINGS["COMMON"]["LOCAL_FAILURE_FILE_PATH"] = (
#     CUSTOM_RUN_SETTINGS["COMMON"]["ROOT_DATA_OUTPUT"]
#     / f"ads_txt_failure_{file_suffix}.parquet"
# )
# CUSTOM_RUN_SETTINGS["COMMON"]["LOCAL_METADATA_FILE_PATH"] = (
#     CUSTOM_RUN_SETTINGS["COMMON"]["ROOT_DATA_OUTPUT"]
#     / f"ads_txt_metadata_{file_suffix}.parquet"
# )

# #######

# CUSTOM_RUN_SETTINGS["COMMON"]["LOCAL_INVENTORY_PARTNER_FILE_PATH"] = (
#     CUSTOM_RUN_SETTINGS["COMMON"]["ROOT_DATA_OUTPUT"]
#     / f"ads_txt_inventory_partner_{file_suffix}.parquet"
# )
# CUSTOM_RUN_SETTINGS["COMMON"]["LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH"] = (
#     CUSTOM_RUN_SETTINGS["COMMON"]["ROOT_DATA_OUTPUT"]
#     / f"ads_txt_inventory_partner_{file_suffix}.xlsx"
# )

# ########


# # Crawl responsibly by identifying yourself (and your website) on the user-agent
# # USER_AGENT = "ads_txt (+http://www.yourdomain.com)"

# # Obey robots.txt rules
# ROBOTSTXT_OBEY = False

# # Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# # Configure a delay for requests for the same website (default: 0)
# # See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# # See also autothrottle settings and docs
# DOWNLOAD_DELAY = 0.3
# RANDOMIZE_DOWNLOAD_DELAY = True  # Random delays for bot evasion
# # The download delay setting will honor only one of:
# # CONCURRENT_REQUESTS_PER_DOMAIN = 16
# # CONCURRENT_REQUESTS_PER_IP = 16

# # Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# # Disable Telnet Console (enabled by default)
# # TELNETCONSOLE_ENABLED = False

# # Override the default request headers:
# # DEFAULT_REQUEST_HEADERS = {
# #    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
# #    "Accept-Language": "en",
# # }

# # Enable or disable spider middlewares
# # See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# # SPIDER_MIDDLEWARES = {
# #    "ads_txt.middlewares.AdsTxtSpiderMiddleware": 543,
# # }

# # Enable or disable downloader middlewares
# # See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#     #    "ads_txt.middlewares.AdsTxtDownloaderMiddleware": 543,
#     "ads_txt.middlewares.DynamicHttpClientMiddleware": 543,
#     "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
#     "scrapy.downloadermiddlewares.retry.RetryMiddleware": 90,
# }

# # Enable or disable extensions
# # See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#     "scrapy.extensions.telnet.TelnetConsole": None,
#     "scrapy.extensions.logstats.LogStats": 500,
#     "scrapy.extensions.corestats.CoreStats": 500,
# }

# # Configure item pipelines
# # See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#     "ads_txt.pipelines.AdsTxtPipeline": 300,
# }

# # Enable and configure the AutoThrottle extension (disabled by default)
# # See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# # The initial download delay
# AUTOTHROTTLE_START_DELAY = 0.5
# # The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 5
# # The average number of requests Scrapy should be sending in parallel to
# # each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 2
# # Enable showing throttling stats for every response received:
# # AUTOTHROTTLE_DEBUG = False

# # Enable and configure HTTP caching (disabled by default)
# # See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# # HTTPCACHE_ENABLED = True
# # HTTPCACHE_EXPIRATION_SECS = 0
# # HTTPCACHE_DIR = "httpcache"
# # HTTPCACHE_IGNORE_HTTP_CODES = []
# # HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# # Set settings whose default value is deprecated to a future-proof value
# TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
# FEED_EXPORT_ENCODING = "utf-8"


# # Enable retry for failed requests
# RETRY_ENABLED = False
# RETRY_TIMES = 1  # Number of retries before failing completely
# RETRY_HTTP_CODES = [500, 502, 503, 504, 403, 429]  # Common bot-blocking status codes

# # Handle different User-Agents for bot evasion
# USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"


# # Configure the logging level (DEBUG for troubleshooting, INFO for production)
# LOG_ENABLED = True  # Ensure logging is enabled
# LOG_LEVEL = "INFO"  # Change to "DEBUG" for more details
# LOG_FILE = CUSTOM_RUN_SETTINGS["COMMON"]["LOG_FILE_PATH"]  # Save logs to a file

# # Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# # Set download timeout (avoid hanging requests)
# DOWNLOAD_TIMEOUT = 10

# # Use HTTP cache for debugging (disabled in production)
# HTTPCACHE_ENABLED = False

# # This will disable the duplicate filter, so Scrapy will re-crawl the same URLs.
# DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter"

# # Email Configuration
# SEND_MAIL_BOOL = True
# MAIL_HOST = "smtp.gmail.com"
# MAIL_PORT = 587
# MAIL_USER = os.getenv("MAIL_USER")
# MAIL_PASS = os.getenv("MAIL_PASS")  # Use App Password, NOT your Gmail password!
# MAIL_FROM = os.getenv("MAIL_FROM")
# MAIL_TO = os.getenv("MAIL_TO")


##############################################
##############################################

# # Scrapy settings for ads_txt project
# import datetime
# from pathlib import Path
# import os
# from collections import UserDict
# import re

# BOT_NAME = "ads_txt"

# SPIDER_MODULES = ["ads_txt.spiders"]
# NEWSPIDER_MODULE = "ads_txt.spiders"

# EXECUTION_DATE = datetime.datetime.now(datetime.timezone.utc)
# CURRENT_WORKING_DIR = Path.cwd()
# utc_date_str = EXECUTION_DATE.strftime("%Y-%m-%d")
# utc_datetime_L1D_str = (EXECUTION_DATE - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
# file_suffix = EXECUTION_DATE.strftime("%Y-%m-%d-%H-%M-%S")


# # ── Dynamic Cloud Saving Dictionary Wrappers ─────────────────────────────────

# class DynamicToGcsDict(UserDict):
#     """Dynamically resolves GCS URIs based on the live LOCAL_SUCCESS_FILE_PATH suffix."""
#     def __init__(self, initial_data, common_dict):
#         super().__init__(initial_data)
#         self.common_dict = common_dict

#     def __getitem__(self, key):
#         val = super().__getitem__(key)
#         local_path = self.common_dict.get("LOCAL_SUCCESS_FILE_PATH")
#         if local_path:
#             filename = os.path.basename(str(local_path))
#             match = re.search(r"ads_txt_success_(.+)\.parquet", filename)
#             if match:
#                 current_suffix = match.group(1)
#                 current_date = Path(local_path).parent.name
                
#                 if key == "GCS_SUCCESS_GRI":
#                     return f"bronze/ads_txt_success/{current_date}/ads_txt_success_{current_suffix}.parquet"
#                 elif key == "GCS_FAILURE_GRI":
#                     return f"bronze/ads_txt_failure/{current_date}/ads_txt_failure_{current_suffix}.parquet"
#                 elif key == "GCS_METADATA_GRI":
#                     return f"bronze/ads_txt_metadata/{current_date}/ads_txt_metadata_{current_suffix}.parquet"
#         return val


# class DynamicToBqDict(UserDict):
#     """Dynamically routes BQ table targets for IPD runs to prevent overwriting first-run tables."""
#     def __init__(self, initial_data, common_dict):
#         super().__init__(initial_data)
#         self.common_dict = common_dict

#     def __getitem__(self, key):
#         val = super().__getitem__(key)
#         local_path = self.common_dict.get("LOCAL_SUCCESS_FILE_PATH")
#         if local_path:
#             filename = os.path.basename(str(local_path))
#             if "_IPD" in filename:
#                 if key == "ADS_TXT_SUCCESS_BQ_TABLE_ID":
#                     return "bronze_all_domains_today_IPD"
#                 elif key == "ADS_TXT_FAILURE_BQ_TABLE_ID":
#                     return "domain_failure_data_today_IPD"
#                 elif key == "ADS_TXT_METADATA_BQ_TABLE_ID":
#                     return "domain_meta_data_IPD"
#         return val


# # ── Construct Common Settings Base ───────────────────────────────────────────

# COMMON_SETTINGS = {
#     "EXECUTION_DATE": file_suffix,
#     "BATCH_SIZE": 100,
#     "ROOT_DATA_OUTPUT": CURRENT_WORKING_DIR / "data_output" / utc_date_str,
#     "ROOT_DATA_OUTPUT_L1D": CURRENT_WORKING_DIR / "data_output" / utc_datetime_L1D_str,
#     "LOG_FILE_PATH": CURRENT_WORKING_DIR / "scrapy_logs" / f"scrapy_log_{file_suffix}.log",
# }

# COMMON_SETTINGS["REMOVE_SAME_DAY_OUTPUT_FILES"] = False
# COMMON_SETTINGS["REMOVE_YESTERDAY_OUTPUT_FILES"] = not COMMON_SETTINGS["REMOVE_SAME_DAY_OUTPUT_FILES"]

# COMMON_SETTINGS["LOCAL_SUCCESS_FILE_PATH"] = (
#     COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_success_{file_suffix}.parquet"
# )
# COMMON_SETTINGS["LOCAL_FAILURE_FILE_PATH"] = (
#     COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_failure_{file_suffix}.parquet"
# )
# COMMON_SETTINGS["LOCAL_METADATA_FILE_PATH"] = (
#     COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_metadata_{file_suffix}.parquet"
# )
# COMMON_SETTINGS["LOCAL_INVENTORY_PARTNER_FILE_PATH"] = (
#     COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_inventory_partner_{file_suffix}.parquet"
# )
# COMMON_SETTINGS["LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH"] = (
#     COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_inventory_partner_{file_suffix}.xlsx"
# )


# # ── Custom Settings Assembly ──────────────────────────────────────────────────

# CUSTOM_RUN_SETTINGS = {
#     "COMMON": COMMON_SETTINGS,
#     "CLOUD_SAVE": {
#         "SERVICE_ACCOUNT_JSON_PATH": CURRENT_WORKING_DIR / ".secrets" / (os.getenv("SERVICE_ACCOUNT_FILE_NAME") or "dummy_sa.json"),
#         "PROJECT_ID": "ads-txt-validator",
#         "TO_GCS": DynamicToGcsDict({
#             "UPLOAD_TO_GCS": False,
#             "GCS_BUCKET": os.getenv("GCS_BUCKET"),
#             "GCS_SUCCESS_GRI": f"bronze/ads_txt_success/{utc_date_str}/ads_txt_success_{file_suffix}.parquet",
#             "GCS_FAILURE_GRI": f"bronze/ads_txt_failure/{utc_date_str}/ads_txt_failure_{file_suffix}.parquet",
#             "GCS_METADATA_GRI": f"bronze/ads_txt_metadata/{utc_date_str}/ads_txt_metadata_{file_suffix}.parquet",
#         }, COMMON_SETTINGS),
#         "TO_BQ": DynamicToBqDict({
#             "UPLOAD_GCS_TO_BQ": False,
#             "BQ_DATASET_ID": os.getenv("BQ_DATASET_ID"),
#             "ADS_TXT_SUCCESS_BQ_TABLE_ID": "bronze_all_domains_today",
#             "ADS_TXT_FAILURE_BQ_TABLE_ID": "domain_failure_data_today",
#             "ADS_TXT_METADATA_BQ_TABLE_ID": "domain_meta_data",
#         }, COMMON_SETTINGS),
#     },
# }


# # ── Standard Scrapy Configuration Rules ──────────────────────────────────────

# ROBOTSTXT_OBEY = False
# CONCURRENT_REQUESTS = 32
# DOWNLOAD_DELAY = 0.3
# RANDOMIZE_DOWNLOAD_DELAY = True
# COOKIES_ENABLED = False

# DOWNLOADER_MIDDLEWARES = {
#     "ads_txt.middlewares.DynamicHttpClientMiddleware": 543,
#     "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
#     "scrapy.downloadermiddlewares.retry.RetryMiddleware": 90,
# }

# EXTENSIONS = {
#     "scrapy.extensions.telnet.TelnetConsole": None,
#     "scrapy.extensions.logstats.LogStats": 500,
#     "scrapy.extensions.corestats.CoreStats": 500,
# }

# ITEM_PIPELINES = {
#     "ads_txt.pipelines.AdsTxtPipeline": 300,
# }

# AUTOTHROTTLE_ENABLED = True
# AUTOTHROTTLE_START_DELAY = 0.5
# AUTOTHROTTLE_MAX_DELAY = 5
# AUTOTHROTTLE_TARGET_CONCURRENCY = 2

# TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
# FEED_EXPORT_ENCODING = "utf-8"

# RETRY_ENABLED = False
# RETRY_TIMES = 1
# RETRY_HTTP_CODES = [500, 502, 503, 504, 403, 429]

# USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"

# LOG_ENABLED = True
# LOG_LEVEL = "INFO"
# LOG_FILE = CUSTOM_RUN_SETTINGS["COMMON"]["LOG_FILE_PATH"]

# TELNETCONSOLE_ENABLED = False
# DOWNLOAD_TIMEOUT = 10
# HTTPCACHE_ENABLED = False
# DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter"

# # Email Configuration
# SEND_MAIL_BOOL = True
# MAIL_HOST = "smtp.gmail.com"
# MAIL_PORT = 587
# MAIL_USER = os.getenv("MAIL_USER")
# MAIL_PASS = os.getenv("MAIL_PASS")
# MAIL_FROM = os.getenv("MAIL_FROM")
# MAIL_TO = os.getenv("MAIL_TO")

####################################################
####################################################

# Scrapy settings for ads_txt project
import datetime
from pathlib import Path
import os
from collections import UserDict
import re

BOT_NAME = "ads_txt"

SPIDER_MODULES = ["ads_txt.spiders"]
NEWSPIDER_MODULE = "ads_txt.spiders"
LOG_LEVEL = "INFO"

ITEM_PIPELINES = {
    # This hooks up your custom pipeline to process, save, and upload the data
    "ads_txt.pipelines.AdsTxtPipeline": 300,
}

EXECUTION_DATE = datetime.datetime.now(datetime.timezone.utc)
CURRENT_WORKING_DIR = Path.cwd()
utc_date_str = EXECUTION_DATE.strftime("%Y-%m-%d")
utc_datetime_L1D_str = (EXECUTION_DATE - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
file_suffix = EXECUTION_DATE.strftime("%Y-%m-%d-%H-%M-%S")


class DynamicToGcsDict(UserDict):
    """Dynamically resolves GCS URIs based on the live LOCAL_SUCCESS_FILE_PATH suffix."""
    def __init__(self, initial_data, common_dict):
        super().__init__(initial_data)
        self.common_dict = common_dict

    def __getitem__(self, key):
        val = super().__getitem__(key)
        local_path = self.common_dict.get("LOCAL_SUCCESS_FILE_PATH")
        if local_path:
            filename = os.path.basename(str(local_path))
            match = re.search(r"ads_txt_success_(.+)\.parquet", filename)
            if match:
                current_suffix = match.group(1)
                current_date = Path(local_path).parent.name
                
                # Dynamic Interception: Fetch prefix if it exists (e.g., "test_run/")
                # If not provided in environment, defaults to an empty string ""
                gcs_prefix = os.getenv("GCS_PATH_PREFIX", "")
                
                if key == "GCS_SUCCESS_GRI":
                    return f"{gcs_prefix}bronze/ads_txt_success/{current_date}/ads_txt_success_{current_suffix}.parquet"
                elif key == "GCS_FAILURE_GRI":
                    return f"{gcs_prefix}bronze/ads_txt_failure/{current_date}/ads_txt_failure_{current_suffix}.parquet"
                elif key == "GCS_METADATA_GRI":
                    return f"{gcs_prefix}bronze/ads_txt_metadata/{current_date}/ads_txt_metadata_{current_suffix}.parquet"
        return val


class DynamicToBqDict(UserDict):
    """Dynamically routes BQ table targets for IPD runs to prevent overwriting first-run tables."""
    def __init__(self, initial_data, common_dict):
        super().__init__(initial_data)
        self.common_dict = common_dict

    def __getitem__(self, key):
        val = super().__getitem__(key)
        local_path = self.common_dict.get("LOCAL_SUCCESS_FILE_PATH")
        if local_path:
            # Convert to string to ensure safe containment checks
            filename = os.path.basename(str(local_path))
            if "_IPD" in filename:
                if key == "ADS_TXT_SUCCESS_BQ_TABLE_ID":
                    return "bronze_all_domains_today_IPD"   # Fixed: plural 'domains'
                elif key == "ADS_TXT_FAILURE_BQ_TABLE_ID":
                    return "domain_failure_data_today_IPD"
                elif key == "ADS_TXT_METADATA_BQ_TABLE_ID":
                    return "domain_meta_data_IPD"
        return val


# ── Construct Common Settings Base ───────────────────────────────────────────

COMMON_SETTINGS = {
    "EXECUTION_DATE": file_suffix,
    "BATCH_SIZE": 100,
    "ROOT_DATA_OUTPUT": CURRENT_WORKING_DIR / "data_output" / utc_date_str,
    "ROOT_DATA_OUTPUT_L1D": CURRENT_WORKING_DIR / "data_output" / utc_datetime_L1D_str,
    "LOG_FILE_PATH": CURRENT_WORKING_DIR / "scrapy_logs" / f"scrapy_log_{file_suffix}.log",
}

COMMON_SETTINGS["REMOVE_SAME_DAY_OUTPUT_FILES"] = False
COMMON_SETTINGS["REMOVE_YESTERDAY_OUTPUT_FILES"] = not COMMON_SETTINGS["REMOVE_SAME_DAY_OUTPUT_FILES"]

COMMON_SETTINGS["LOCAL_SUCCESS_FILE_PATH"] = (
    COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_success_{file_suffix}.parquet"
)
COMMON_SETTINGS["LOCAL_FAILURE_FILE_PATH"] = (
    COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_failure_{file_suffix}.parquet"
)
COMMON_SETTINGS["LOCAL_METADATA_FILE_PATH"] = (
    COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_metadata_{file_suffix}.parquet"
)
COMMON_SETTINGS["LOCAL_INVENTORY_PARTNER_FILE_PATH"] = (
    COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_inventory_partner_{file_suffix}.parquet"
)
COMMON_SETTINGS["LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH"] = (
    COMMON_SETTINGS["ROOT_DATA_OUTPUT"] / f"ads_txt_inventory_partner_{file_suffix}.xlsx"
)


# ── Custom Settings Assembly ──────────────────────────────────────────────────

CUSTOM_RUN_SETTINGS = {
    "COMMON": COMMON_SETTINGS,
    "CLOUD_SAVE": {
        "SERVICE_ACCOUNT_JSON_PATH": CURRENT_WORKING_DIR / ".secrets" / (os.getenv("SERVICE_ACCOUNT_FILE_NAME") or "dummy_sa.json"),
        "PROJECT_ID": "ads-txt-validator",
        "TO_GCS": DynamicToGcsDict({
            "UPLOAD_TO_GCS": os.getenv("UPLOAD_TO_GCS", "False").lower() == "true",
            "GCS_BUCKET": os.getenv("GCS_BUCKET"),
            "GCS_SUCCESS_GRI": f"bronze/ads_txt_success/{utc_date_str}/ads_txt_success_{file_suffix}.parquet",
            "GCS_FAILURE_GRI": f"bronze/ads_txt_failure/{utc_date_str}/ads_txt_failure_{file_suffix}.parquet",
            "GCS_METADATA_GRI": f"bronze/ads_txt_metadata/{utc_date_str}/ads_txt_metadata_{file_suffix}.parquet",
        }, COMMON_SETTINGS),
        "TO_BQ": DynamicToBqDict({
            "UPLOAD_GCS_TO_BQ": os.getenv("UPLOAD_GCS_TO_BQ", "False").lower() == "true",
            "BQ_DATASET_ID": os.getenv("BQ_DATASET_ID"),
            "ADS_TXT_SUCCESS_BQ_TABLE_ID": "bronze_all_domains_today",
            "ADS_TXT_FAILURE_BQ_TABLE_ID": "domain_failure_data_today",
            "ADS_TXT_METADATA_BQ_TABLE_ID": "domain_meta_data",
        }, COMMON_SETTINGS),
    },
}