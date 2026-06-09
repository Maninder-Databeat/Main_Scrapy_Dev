# from twisted.internet import reactor, defer
# from scrapy.crawler import CrawlerRunner
# from scrapy.utils.log import configure_logging

# import asyncio

# # Windows uses ProactorEventLoop by default — force SelectorEventLoop for Twisted
# asyncio.DefaultEventLoopPolicy = asyncio.WindowsSelectorEventLoopPolicy
# loop = asyncio.SelectorEventLoop()
# asyncio.set_event_loop(loop)

# from twisted.internet import asyncioreactor
# asyncioreactor.install(loop)

# import datetime
# import os
# from collections import defaultdict
# # from pprint import pprint

# import certifi
# from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings
# from dotenv import load_dotenv
# # import pandas as pd

# from ads_txt.utils import GoogleServices
# from loguru_loggins import get_logger

# import pandas as pd
# import glob

# import sys
# from scrapy.crawler import CrawlerRunner
# from twisted.internet import reactor, defer
# from scrapy.utils.log import configure_logging

# load_dotenv(r".secrets/.env")
# logger = get_logger()

# # Manually set the Scrapy project environment
# os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "ads_txt.settings")
# os.environ["SSL_CERT_FILE"] = certifi.where()
# EXECUTION_DATE = datetime.datetime.now(datetime.timezone.utc)
# file_suffix = EXECUTION_DATE.strftime("%Y-%m-%d-%H-%M-%S")
# # print(f"{certifi.where()=}")

# from ads_txt.spiders.scraper_spider import ScraperSpider


# def run_spider(start_urls_with_meta, project_setting, is_test=True):
#     # Override settings dynamically (optional)
#     # project_setting.set("LOG_LEVEL", "INFO")
#     # project_setting.set("FEEDS", {"output.json": {"format": "json"}})

#     # start_urls = ["https://tls.browserleaks.com/json"]
#     if is_test:
#         project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_GCS"][
#             "UPLOAD_TO_GCS"
#         ] = False
#         project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"][
#             "UPLOAD_GCS_TO_BQ"
#         ] = False
#         start_urls_with_meta = [
#             (
#                 "yahoo.com",
#                 defaultdict(
#                     None,
#                     {
#                         "http_client": "curl_cffi",
#                         "inventory_type": "app-ads.txt",
#                         "ads_txt_page_url": "",
#                     },
#                 ),
#             ),
#             # (
#             #     "www.citationmachine.net",
#             #     defaultdict(
#             #         None,
#             #         {
#             #             "http_client": "curl_cffi",
#             #             "inventory_type": "ads.txt",
#             #             "ads_txt_page_url": "https://www.citationmachine.net/a.txt",
#             #         },
#             #     ),
#             # ),
#             # (
#             #     "thegazette.com",
#             #     defaultdict(
#             #         None,
#             #         {
#             #             "http_client": "default",
#             #             "inventory_type": "app-ads.txt",
#             #             "ads_txt_page_url": "",
#             #         },
#             #     ),
#             # ),
#             # (
#             #     "notfound.com",
#             #     defaultdict(
#             #         None,
#             #         {
#             #             "http_client": "default",
#             #             "inventory_type": "app-ads.txt",
#             #             "ads_txt_page_url": "",
#             #         },
#             #     ),
#             # ),
#             # (
#             #     "notfoundssss.com",
#             #     defaultdict(
#             #         None,
#             #         {
#             #             "http_client": "default",
#             #             "inventory_type": "app-ads.txt",
#             #             "ads_txt_page_url": "",
#             #         },
#             #     ),
#             # ),
#         ]

#     # Start the Scrapy crawler
#     process = CrawlerProcess(project_setting)
#     process.crawl(ScraperSpider, start_urls_with_meta=start_urls_with_meta)
#     process.start()


# # if __name__ == "__main__":


# #     logger.info("Spider crawl started")
# #     # Now get_project_settings() will load settings from settings.py
# #     settings = get_project_settings()

# #     execution_date: str = datetime.datetime.now(datetime.timezone.utc)

# #     settings.set("EXECUTION_DATE", execution_date)

# #     logger.info(settings.copy_to_dict())

# #     project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
# #     service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"][
# #         "SERVICE_ACCOUNT_JSON_PATH"
# #     ]

# #     try:
# #         gservice = GoogleServices(project_id, service_account_file_path)

# #         query = """ SELECT domain, inventory_type, ads_page_url, http_client FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table` """
# #         df = gservice.query_bq_to_pd(query)

# #         # If google sheet input is required. (Do Not change this file, make copy of this one)
# #         # google_sheet_id = "1DLevo_k0LTfIHH7wL9n08NfYnpq_CbSF5RLNIMaIJU8"
# #         # worksheet_name = "main_file"

# #         # df = gservice.adhoc_gsheet_pull(google_sheet_id, worksheet_name)

# #         df.drop(
# #             columns=[
# #                 col_name
# #                 for col_name in df.columns
# #                 if col_name
# #                 not in ["http_client", "ads_page_url", "domain", "inventory_type"]
# #             ],
# #             inplace=True,
# #         )

# #         df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
        
# #     finally:
# #         gservice.service_close()

# #     logger.info(df)
# #     logger.info(df.dtypes)

# #     # df = df.head(10)
# #     try:
# #         df.fillna(
# #             {
# #                 "http_client": "curl_cffi",
# #                 "ads_page_url": "",
# #                 "domain": "NA",
# #                 "inventory_type": "ads.txt",
# #             },
# #             inplace=True,
# #         )
# #         # df.fillna({"ads_page_url": ""}, inplace=True)
# #         # df.fillna({"domain": "NA"}, inplace=True)
# #     except Exception as e:
# #         logger.error(e)
# #         df["http_client"].fillna("curl_cffi", inplace=True)
# #         df["ads_page_url"].fillna("", inplace=True)

# #     logger.info(df)

# #     # df = pd.read_csv(r"C:\Users\zaid\Downloads\sovrn_web.csv")
# #     start_urls_with_meta = [
# #         (
# #             row["domain"],  # ✅ Use domain instead of index
# #             defaultdict(
# #                 None,
# #                 {
# #                     "http_client": row["http_client"],
# #                     "inventory_type": row["inventory_type"],
# #                     "ads_txt_page_url": row["ads_page_url"],
# #                 },
# #             ),
# #         )
# #         for _, row in df.iterrows()
# #     ]
# #     del df
# #     run_spider(start_urls_with_meta, project_setting=settings, is_test=True)
# #     logger.info("Spider crawl finished")


# if __name__ == "__main__":
#     is_test = True  # True=local/test path, False=production path

#     logger.info("Spider crawl started")
#     settings = get_project_settings()
#     execution_date = datetime.datetime.now(datetime.timezone.utc)
#     settings.set("EXECUTION_DATE", execution_date)
#     logger.info(settings.copy_to_dict())

#     start_urls_with_meta = []

#     if not is_test:
#         project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
#         service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["SERVICE_ACCOUNT_JSON_PATH"]

#         gservice = GoogleServices(project_id, service_account_file_path)
#         try:
#             query = """ SELECT domain, inventory_type, ads_page_url, http_client FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table` """
#             df = gservice.query_bq_to_pd(query)

#                     # If google sheet input is required. (Do Not change this file, make copy of this one)
#                 # google_sheet_id = "1DLevo_k0LTfIHH7wL9n08NfYnpq_CbSF5RLNIMaIJU8"
#                 # worksheet_name = "main_file"

#                 # df = gservice.adhoc_gsheet_pull(google_sheet_id, worksheet_name)

#             df.drop(
#                     columns=[
#                         col_name
#                         for col_name in df.columns
#                         if col_name
#                         not in ["http_client", "ads_page_url", "domain", "inventory_type"]
#                     ],
#                     inplace=True,
#                 )

#             df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
                
#         finally:
#             gservice.service_close()

#         logger.info(df)
#         logger.info(df.dtypes)

#             # df = df.head(10)
#         try:
#             df.fillna(
#                 {
#                     "http_client": "curl_cffi",
#                     "ads_page_url": "",
#                     "domain": "NA",
#                     "inventory_type": "ads.txt",
#                 },
#                 inplace=True,
#             )
#             # df.fillna({"ads_page_url": ""}, inplace=True)
#             # df.fillna({"domain": "NA"}, inplace=True)
#         except Exception as e:
#             logger.error(e)
#             df["http_client"].fillna("curl_cffi", inplace=True)
#             df["ads_page_url"].fillna("", inplace=True)

#         logger.info(df)

#         # df = pd.read_csv(r"C:\Users\zaid\Downloads\sovrn_web.csv")
        
#         start_urls_with_meta = [
#             (
#                 row["domain"],
#                 defaultdict(None, {
#                     "http_client": row["http_client"],
#                     "inventory_type": row["inventory_type"],
#                     "ads_txt_page_url": row["ads_page_url"],
#                 }),
#             )
#             for _, row in df.iterrows()
#         ]
#         # finally:gservice.service_close()

#     else:
#         logger.info("Test mode active: skipping GCS/BQ service-account section")

#     # run_spider(start_urls_with_meta, project_setting=settings, is_test=is_test)
#     # logger.info("Spider crawl finished")

#     ########################################

#    # --- Resolve final URLs for first run (apply is_test overrides) ---
#     first_run_urls = run_spider(start_urls_with_meta, project_setting=settings, is_test=is_test)

#     # --- Resolve second run URLs ---
#     second_run_urls = []
#     logger.info("🔍 Looking for ads_txt_inventory_partner Excel files...")
#     inventory_files = glob.glob("**/ads_txt_inventory_partner*.xlsx", recursive=True)
#     if inventory_files:
#         latest_inventory_file = max(inventory_files, key=os.path.getctime)
#         logger.info(f"📊 Found inventory partner file: {latest_inventory_file}")
#         df_ipd = pd.read_excel(latest_inventory_file)
#         unique_ip_domains = df_ipd["inventory_partner_domain"].dropna().str.strip().unique()
#         logger.info(f"🎯 Found {len(unique_ip_domains)} unique inventory partner domains")
#         second_run_urls = [
#             (
#                 domain,
#                 defaultdict(None, {
#                     "http_client": "curl_cffi",
#                     "inventory_type": "ads.txt",
#                     "ads_txt_page_url": "",
#                 }),
#             )
#             for domain in unique_ip_domains
#         ]
#     else:
#         logger.warning("⚠️ No ads_txt_inventory_partner*.xlsx file found - skipping second run")

#     # --- Use CrawlerRunner to allow multiple sequential runs in one reactor ---
#     configure_logging()
#     runner = CrawlerRunner(settings)

#     @defer.inlineCallbacks
#     def crawl_sequence():
#         logger.info("🚀 Starting FIRST spider run (original domains)")
#         yield runner.crawl(ScraperSpider, start_urls_with_meta=first_run_urls)

#         if second_run_urls:
#             settings.set("EXECUTION_DATE", f"{execution_date}_IPD")
#             logger.info("🚀 Starting SECOND spider run (inventory partner domains only)")
#             yield runner.crawl(ScraperSpider, start_urls_with_meta=second_run_urls)

#         reactor.stop()

#     crawl_sequence()
#     reactor.run()  # Blocks here until reactor.stop() is called above

#     logger.info("✅ All spider crawls finished")


##################################
##################################

# import asyncio

# # Windows uses ProactorEventLoop by default — force SelectorEventLoop for Twisted
# asyncio.DefaultEventLoopPolicy = asyncio.WindowsSelectorEventLoopPolicy
# loop = asyncio.SelectorEventLoop()
# asyncio.set_event_loop(loop)

# from twisted.internet import asyncioreactor
# asyncioreactor.install(loop)

# # ── All other imports AFTER reactor install ──────────────────────────────────

# import datetime
# import os
# import sys
# import glob
# from collections import defaultdict

# import certifi
# from scrapy.crawler import CrawlerRunner
# from scrapy.utils.project import get_project_settings
# from scrapy.utils.log import configure_logging
# from twisted.internet import reactor, defer
# from dotenv import load_dotenv

# from ads_txt.utils import GoogleServices
# from loguru_loggins import get_logger
# import pandas as pd
# from pathlib import Path

# load_dotenv(r".secrets/.env")
# logger = get_logger()

# os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "ads_txt.settings")
# os.environ["SSL_CERT_FILE"] = certifi.where()

# from ads_txt.spiders.scraper_spider import ScraperSpider


# # def get_test_urls(project_setting):
# #     """Apply test-mode setting overrides and return hardcoded test URLs."""
# #     project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_GCS"]["UPLOAD_TO_GCS"] = False
# #     project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"]["UPLOAD_GCS_TO_BQ"] = False
# #     return [
# #         (
# #             "yahoo.com",
# #             defaultdict(None, {
# #                 "http_client": "curl_cffi",
# #                 "inventory_type": "app-ads.txt",
# #                 "ads_txt_page_url": "",
# #             }),
# #         ),
# #     ]

# def get_test_urls(project_setting):
#     """Respects environment configuration variables for GCS/BQ even during test runs."""
#     cloud_settings = project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]
    
#     # Read directly from environment strings, defaulting to False if not present
#     cloud_settings["TO_GCS"]["UPLOAD_TO_GCS"] = os.getenv("UPLOAD_TO_GCS", "False").lower() == "true"
#     cloud_settings["TO_BQ"]["UPLOAD_GCS_TO_BQ"] = os.getenv("UPLOAD_GCS_TO_BQ", "False").lower() == "true"
    
#     return [
#         (
#             "yahoo.com",
#             defaultdict(None, {
#                 "http_client": "curl_cffi",
#                 "inventory_type": "app-ads.txt",
#                 "ads_txt_page_url": "",
#             }),
#         ),
#     ]


# if __name__ == "__main__":
#     # is_test = True  # True=local/test path, False=production path
#     is_test = os.getenv("IS_TEST")

#     logger.info("Spider crawl started")
#     settings = get_project_settings()
#     execution_date = datetime.datetime.now(datetime.timezone.utc)
#     settings.set("EXECUTION_DATE", execution_date)
#     logger.info(settings.copy_to_dict())

#     # ── Build first run URLs ─────────────────────────────────────────────────
#     if is_test:
#         logger.info("Test mode active: skipping GCS/BQ service-account section")
#         first_run_urls = get_test_urls(settings)

#     else:
#         project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
#         service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["SERVICE_ACCOUNT_JSON_PATH"]

#         gservice = GoogleServices(project_id, service_account_file_path)
#         try:
#             query = """
#                 SELECT domain, inventory_type, ads_page_url, http_client
#                 FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table`
#             """
#             df = gservice.query_bq_to_pd(query)
#             df.drop(
#                 columns=[c for c in df.columns if c not in ["http_client", "ads_page_url", "domain", "inventory_type"]],
#                 inplace=True,
#             )
#             df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
#         finally:
#             gservice.service_close()

#         logger.info(df)
#         logger.info(df.dtypes)

#         try:
#             df.fillna(
#                 {"http_client": "curl_cffi", "ads_page_url": "", "domain": "NA", "inventory_type": "ads.txt"},
#                 inplace=True,
#             )
#         except Exception as e:
#             logger.error(e)
#             df["http_client"].fillna("curl_cffi", inplace=True)
#             df["ads_page_url"].fillna("", inplace=True)

#         logger.info(df)

#         first_run_urls = [
#             (
#                 row["domain"],
#                 defaultdict(None, {
#                     "http_client": row["http_client"],
#                     "inventory_type": row["inventory_type"],
#                     "ads_txt_page_url": row["ads_page_url"],
#                 }),
#             )
#             for _, row in df.iterrows()
#         ]

#     # ── Build second run URLs (from inventory partner Excel) ─────────────────
#     second_run_urls = []
#     logger.info("🔍 Looking for ads_txt_inventory_partner Excel files...")
#     inventory_files = glob.glob("**/ads_txt_inventory_partner*.xlsx", recursive=True)
#     if inventory_files:
#         latest_inventory_file = max(inventory_files, key=os.path.getctime)
#         logger.info(f"📊 Found inventory partner file: {latest_inventory_file}")
#         df_ipd = pd.read_excel(latest_inventory_file)
#         unique_ip_domains = df_ipd["inventory_partner_domain"].dropna().str.strip().unique()
#         logger.info(f"🎯 Found {len(unique_ip_domains)} unique inventory partner domains")
#         second_run_urls = [
#             (
#                 domain,
#                 defaultdict(None, {
#                     "http_client": "curl_cffi",
#                     "inventory_type": "ads.txt",
#                     "ads_txt_page_url": "",
#                 }),
#             )
#             for domain in unique_ip_domains
#         ]
#     else:
#         logger.warning("⚠️ No ads_txt_inventory_partner*.xlsx file found - skipping second run")

#     # ── Run spiders sequentially using CrawlerRunner (single reactor) ────────
#     configure_logging(settings)
#     runner = CrawlerRunner(settings)

#     # @defer.inlineCallbacks
#     # def crawl_sequence():
#     #     logger.info("🚀 Starting FIRST spider run (original domains)")
#     #     yield runner.crawl(ScraperSpider, start_urls_with_meta=first_run_urls)
#     #     logger.info("✅ First spider run complete")

#     #     if second_run_urls:
#     #         settings.set("EXECUTION_DATE", f"{execution_date}_IPD")
#     #         logger.info("🚀 Starting SECOND spider run (inventory partner domains only)")
#     #         yield runner.crawl(ScraperSpider, start_urls_with_meta=second_run_urls)
#     #         logger.info("✅ Second spider run complete")
#     #     else:
#     #         logger.warning("⚠️ No second run URLs — skipping")

#     # d = crawl_sequence()
#     # d.addErrback(lambda f: logger.error(f"❌ Crawl sequence error: {f}"))
#     # d.addBoth(lambda _: reactor.stop())  # Always stop reactor, success or failure

#     # reactor.run()  # Single blocking call — stops when crawl_sequence finishes
#     # logger.info("✅ All spider crawls finished")

#     @defer.inlineCallbacks
#     def crawl_sequence():
#         logger.info("🚀 Starting FIRST spider run (original domains)")
#         yield runner.crawl(ScraperSpider, start_urls_with_meta=first_run_urls)
#         logger.info("✅ First spider run complete")

#         # --- NEW: Move IPD discovery logic HERE ---
#         second_run_urls = []
#         logger.info("🔍 Looking for newly created ads_txt_inventory_partner Excel files...")
#         inventory_files = glob.glob("**/ads_txt_inventory_partner*.xlsx", recursive=True)
        
#         if inventory_files:
#             latest_inventory_file = max(inventory_files, key=os.path.getctime)
#             df_ipd = pd.read_excel(latest_inventory_file)
#             unique_ip_domains = df_ipd["inventory_partner_domain"].dropna().str.strip().unique()
            
#             second_run_urls = [
#                 (domain, defaultdict(None, {
#                     "http_client": "curl_cffi",
#                     "inventory_type": "ads.txt",
#                     "ads_txt_page_url": "",
#                 })) for domain in unique_ip_domains
#             ]

#         if second_run_urls:
#             # ── Generate a new timestamp suffix for the second run ────────────
#             ipd_execution_date = datetime.datetime.now(datetime.timezone.utc)
#             ipd_suffix = ipd_execution_date.strftime("%Y-%m-%d-%H-%M-%S") + "_IPD"
#             utc_date_str = ipd_execution_date.strftime("%Y-%m-%d")
#             root_output = Path.cwd() / "data_output" / utc_date_str

#             # ── Patch CUSTOM_RUN_SETTINGS with new IPD-specific paths ─────────
#             # The moment this key updates with '_IPD', DynamicToBqDict will instantly auto-route your BQ tables!
#             settings.get("CUSTOM_RUN_SETTINGS")["COMMON"].update({
#                 "LOCAL_SUCCESS_FILE_PATH":  root_output / f"ads_txt_success_{ipd_suffix}.parquet",
#                 "LOCAL_FAILURE_FILE_PATH":  root_output / f"ads_txt_failure_{ipd_suffix}.parquet",
#                 "LOCAL_METADATA_FILE_PATH": root_output / f"ads_txt_metadata_{ipd_suffix}.parquet",
#                 "LOCAL_INVENTORY_PARTNER_FILE_PATH":  root_output / f"ads_txt_inventory_partner_{ipd_suffix}.parquet",
#                 "LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH": root_output / f"ads_txt_inventory_partner_{ipd_suffix}.xlsx",
#             })

#             # REMOVE OR COMMENT OUT THIS LINE:
#             # settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"]["ADS_TXT_SUCCESS_BQ_TABLE_ID"] = "bronze_all_domains_today_IPD"

#             settings.set("EXECUTION_DATE", ipd_execution_date)

#     d = crawl_sequence()
#     d.addErrback(lambda f: logger.error(f"❌ Crawl sequence error: {f}"))
#     d.addBoth(lambda _: reactor.stop())  # Always stop reactor, success or failure

#     reactor.run()  # Single blocking call — stops when crawl_sequence finishes
#     logger.info("✅ All spider crawls finished")


##############################
##############################

# import asyncio

# # Windows uses ProactorEventLoop by default — force SelectorEventLoop for Twisted
# asyncio.DefaultEventLoopPolicy = asyncio.WindowsSelectorEventLoopPolicy
# loop = asyncio.SelectorEventLoop()
# asyncio.set_event_loop(loop)

# from twisted.internet import asyncioreactor
# asyncioreactor.install(loop)

# # ── All other imports AFTER reactor install ──────────────────────────────────

# import datetime
# import os
# import sys
# import glob
# from collections import defaultdict

# import certifi
# from scrapy.crawler import CrawlerRunner
# from scrapy.utils.project import get_project_settings
# from scrapy.utils.log import configure_logging
# from twisted.internet import reactor, defer
# from dotenv import load_dotenv

# from ads_txt.utils import GoogleServices
# from loguru_loggins import get_logger
# import pandas as pd
# from pathlib import Path

# load_dotenv(r".secrets/.env")
# logger = get_logger()

# os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "ads_txt.settings")
# os.environ["SSL_CERT_FILE"] = certifi.where()

# from ads_txt.spiders.scraper_spider import ScraperSpider


# def get_test_urls(project_setting):
#     """Respects environment configuration variables for GCS/BQ even during test runs."""
#     cloud_settings = project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]
    
#     # Read directly from environment strings, defaulting to False if not present
#     cloud_settings["TO_GCS"]["UPLOAD_TO_GCS"] = os.getenv("UPLOAD_TO_GCS", "False").lower() == "true"
#     cloud_settings["TO_BQ"]["UPLOAD_GCS_TO_BQ"] = os.getenv("UPLOAD_GCS_TO_BQ", "False").lower() == "true"
    
#     return [
#         (
#             "yahoo.com",
#             defaultdict(None, {
#                 "http_client": "curl_cffi",
#                 "inventory_type": "app-ads.txt",
#                 "ads_txt_page_url": "",
#             }),
#         ),
#     ]


# if __name__ == "__main__":
#     # is_test = os.getenv("IS_TEST")
#     is_test = False

#     logger.info("Spider crawl started")
#     settings = get_project_settings()
#     execution_date = datetime.datetime.now(datetime.timezone.utc)
#     settings.set("EXECUTION_DATE", execution_date)

#     # ── 💡 ADD THIS CODE HERE TO ENFORCE TABLE REROUTING ─────────────────────
#     from ads_txt.settings import DynamicToBqDict
    
#     # Wrap the standard BQ settings dictionary in your dynamic proxy router
#     common_cfg = settings.get("CUSTOM_RUN_SETTINGS")["COMMON"]
#     bq_cfg = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"]
    
#     settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"] = DynamicToBqDict(bq_cfg, common_cfg)


#     logger.info(settings.copy_to_dict())

#     # ── Build first run URLs ─────────────────────────────────────────────────
#     if is_test:
#         logger.info("Test mode active: skipping GCS/BQ service-account section")
#         first_run_urls = get_test_urls(settings)

#     else:
#         project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
#         service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["SERVICE_ACCOUNT_JSON_PATH"]

#         gservice = GoogleServices(project_id, service_account_file_path)
#         try:
#             query = """
#                 SELECT domain, inventory_type, ads_page_url, http_client
#                 FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table`
#                 LIMIT 100
#             """
#             df = gservice.query_bq_to_pd(query)
#             df.drop(
#                 columns=[c for c in df.columns if c not in ["http_client", "ads_page_url", "domain", "inventory_type"]],
#                 inplace=True,
#             )
#             df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
#         finally:
#             gservice.service_close()

#         logger.info(df)
#         logger.info(df.dtypes)

#         try:
#             df.fillna(
#                 {"http_client": "curl_cffi", "ads_page_url": "", "domain": "NA", "inventory_type": "ads.txt"},
#                 inplace=True,
#             )
#         except Exception as e:
#             logger.error(e)
#             df["http_client"].fillna("curl_cffi", inplace=True)
#             df["ads_page_url"].fillna("", inplace=True)

#         logger.info(df)

#         first_run_urls = [
#             (
#                 row["domain"],
#                 defaultdict(None, {
#                     "http_client": row["http_client"],
#                     "inventory_type": row["inventory_type"],
#                     "ads_txt_page_url": row["ads_page_url"],
#                 }),
#             )
#             for _, row in df.iterrows()
#         ]

#     # ── Run spiders sequentially using CrawlerRunner (single reactor) ────────
#     configure_logging(settings)
#     runner = CrawlerRunner(settings)

#     @defer.inlineCallbacks
#     def crawl_sequence():
#         # ── 1. EXECUTE THE FIRST RUN SPIDER ──────────────────────────────────
#         logger.info("🚀 Starting FIRST spider run (original domains)")
#         yield runner.crawl(ScraperSpider, start_urls_with_meta=first_run_urls)
#         logger.info("✅ First spider run complete")

#         # ── 2. DYNAMICALLY DISCOVER THE GENERATED EXCEL FILE ─────────────────
#         second_run_urls = []
#         logger.info("🔍 Looking for newly created ads_txt_inventory_partner Excel files...")
        
#         # Pull the exact target path built by the current run settings block
#         current_run_excel = settings.get("CUSTOM_RUN_SETTINGS")["COMMON"]["LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH"]

#         if os.path.exists(current_run_excel):
#             logger.info(f"📊 Found active session partner file: {current_run_excel}")
#             df_ipd = pd.read_excel(current_run_excel)
#         else:
#             # Fallback pathing query: Fetch the absolute freshest file on local disk via modification time
#             inventory_files = glob.glob("**/ads_txt_inventory_partner*.xlsx", recursive=True)
#             if inventory_files:
#                 latest_inventory_file = max(inventory_files, key=os.path.getmtime)
#                 logger.info(f"📊 Fallback found latest partner file on disk: {latest_inventory_file}")
#                 df_ipd = pd.read_excel(latest_inventory_file)
#             else:
#                 logger.warning("⚠️ No inventory partner files found on disk.")
#                 df_ipd = pd.DataFrame()

#         # Parse unique target domains out safely
#         if not df_ipd.empty and "inventory_partner_domain" in df_ipd.columns:
#             unique_ip_domains = df_ipd["inventory_partner_domain"].dropna().str.strip().unique()
#             unique_ip_domains = [d for d in unique_ip_domains if d] # Clear blank lines
#             logger.info(f"🎯 Found {len(unique_ip_domains)} unique inventory partner domains")
            
#             second_run_urls = [
#                 (domain, defaultdict(None, {
#                     "http_client": "curl_cffi",
#                     "inventory_type": "ads.txt",
#                     "ads_txt_page_url": "",
#                 })) for domain in unique_ip_domains
#             ]
#         else:
#             logger.warning("⚠️ Inventory DataFrame empty or missing required schema columns.")

#         # ── 3. EXECUTE THE SECOND RUN SPIDER (IF TARGETS FOUND) ──────────────
#         if second_run_urls:
#             # Generate secondary runtime suffix strings
#             ipd_execution_date = datetime.datetime.now(datetime.timezone.utc)
#             ipd_suffix = ipd_execution_date.strftime("%Y-%m-%d-%H-%M-%S") + "_IPD"
#             utc_date_str = ipd_execution_date.strftime("%Y-%m-%d")
#             root_output = Path.cwd() / "data_output" / utc_date_str

#             # Remap workspace configurations. The '_IPD' tag will trip DynamicToBqDict routing
#             settings.get("CUSTOM_RUN_SETTINGS")["COMMON"].update({
#                 "LOCAL_SUCCESS_FILE_PATH":  root_output / f"ads_txt_success_{ipd_suffix}.parquet",
#                 "LOCAL_FAILURE_FILE_PATH":  root_output / f"ads_txt_failure_{ipd_suffix}.parquet",
#                 "LOCAL_METADATA_FILE_PATH": root_output / f"ads_txt_metadata_{ipd_suffix}.parquet",
#                 "LOCAL_INVENTORY_PARTNER_FILE_PATH":  root_output / f"ads_txt_inventory_partner_{ipd_suffix}.parquet",
#                 "LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH": root_output / f"ads_txt_inventory_partner_{ipd_suffix}.xlsx",
#             })

#             settings.set("EXECUTION_DATE", ipd_execution_date)

#             logger.info("🚀 Starting SECOND spider run (inventory partner domains only)")
#             yield runner.crawl(ScraperSpider, start_urls_with_meta=second_run_urls)
#             logger.info("✅ Second spider run complete")
#         else:
#             logger.warning("⚠️ Skipping second crawl sequence: No domains found to parse.")

#     # Execute the asynchronous sequence chains inside the event loop
#     d = crawl_sequence()
#     d.addErrback(lambda f: logger.error(f"❌ Crawl sequence error: {f}"))
#     d.addBoth(lambda _: reactor.stop())

#     reactor.run()
#     logger.info("✅ All spider crawls finished")


################################################
# Working but taking IPD from prev day if no file present today
##################################################


import asyncio

# Windows uses ProactorEventLoop by default — force SelectorEventLoop for Twisted
asyncio.DefaultEventLoopPolicy = asyncio.WindowsSelectorEventLoopPolicy
loop = asyncio.SelectorEventLoop()
asyncio.set_event_loop(loop)

from twisted.internet import asyncioreactor
asyncioreactor.install(loop)

# ── All other imports AFTER reactor install ──────────────────────────────────

import datetime
import os
import sys
import glob
from collections import defaultdict

import certifi
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from twisted.internet import reactor, defer
from dotenv import load_dotenv

from ads_txt.utils import GoogleServices
from loguru_loggins import get_logger
import pandas as pd
from pathlib import Path

load_dotenv(r".secrets/.env")
logger = get_logger()

os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "ads_txt.settings")
os.environ["SSL_CERT_FILE"] = certifi.where()

from ads_txt.spiders.scraper_spider import ScraperSpider


def get_test_urls(project_setting):
    """Respects environment configuration variables for GCS/BQ even during test runs."""
    cloud_settings = project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]
    
    # Read directly from environment strings, defaulting to False if not present
    cloud_settings["TO_GCS"]["UPLOAD_TO_GCS"] = os.getenv("UPLOAD_TO_GCS", "False").lower() == "true"
    cloud_settings["TO_BQ"]["UPLOAD_GCS_TO_BQ"] = os.getenv("UPLOAD_GCS_TO_BQ", "False").lower() == "true"
    
    return [
        (
            "yahoo.com",
            defaultdict(None, {
                "http_client": "curl_cffi",
                "inventory_type": "app-ads.txt",
                "ads_txt_page_url": "",
            }),
        ),
    ]


if __name__ == "__main__":
    # is_test = os.getenv("IS_TEST")
    is_test = False

    logger.info("Spider crawl started")
    settings = get_project_settings()
    execution_date = datetime.datetime.now(datetime.timezone.utc)
    settings.set("EXECUTION_DATE", execution_date)

# ── 🎯 FIX: THROTTLE CONCURRENCY TO PREVENT WINDOWS SELECT OVERFLOW ──
    import platform
    if platform.system() == "Windows":
        logger.warning("🪟 Windows OS detected local execution. Applying 64-socket limits safeguards.")
        
        # 1. Clamp down concurrency tightly
        settings.set("CONCURRENT_REQUESTS", 12)           # Safe baseline for active downloads
        settings.set("CONCURRENT_REQUESTS_PER_DOMAIN", 2)
        settings.set("REACTOR_THREADPOOL_MAXSIZE", 4)      # Keeps internal Twisted thread sockets lean
        
        # 2. Turn on AutoThrottle to smooth out spikes
        settings.set("AUTOTHROTTLE_ENABLED", True)
        settings.set("AUTOTHROTTLE_START_DELAY", 1.0)
        settings.set("AUTOTHROTTLE_MAX_DELAY", 5.0)
        
        # 3. 💥 THE CRITICAL FIX: Kill Connection Pooling
        # Forces curl_cffi/Scrapy to drop the network socket instantly upon completion
        # instead of holding it open in a background keep-alive pool.
        settings.set("DEFAULT_REQUEST_HEADERS", {
            "Connection": "close"
        })
    else:
        logger.info("☁️ Linux/Cloud environment detected. Running at native production speeds.")
    # ─────────────────────────────────────────────────────────────────────
    # ── 💡 ADD THIS CODE HERE TO ENFORCE TABLE REROUTING ─────────────────────
    from ads_txt.settings import DynamicToBqDict
    
    # Wrap the standard BQ settings dictionary in your dynamic proxy router
    common_cfg = settings.get("CUSTOM_RUN_SETTINGS")["COMMON"]
    bq_cfg = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"]
    
    settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"] = DynamicToBqDict(bq_cfg, common_cfg)


    logger.info(settings.copy_to_dict())

    # ── Build first run URLs ─────────────────────────────────────────────────
    if is_test:
        logger.info("Test mode active: skipping GCS/BQ service-account section")
        first_run_urls = get_test_urls(settings)

    else:
        project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
        service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["SERVICE_ACCOUNT_JSON_PATH"]

        gservice = GoogleServices(project_id, service_account_file_path)
        try:
            query = """
                SELECT domain, inventory_type, ads_page_url, http_client
                FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table`
            """
            df = gservice.query_bq_to_pd(query)
            df.drop( 
                columns=[c for c in df.columns if c not in ["http_client", "ads_page_url", "domain", "inventory_type"]],
                inplace=True,
            )
            df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
        finally:
            gservice.service_close()

        logger.info(df)
        logger.info(df.dtypes)

        try:
            df.fillna(
                {"http_client": "curl_cffi", "ads_page_url": "", "domain": "NA", "inventory_type": "ads.txt"},
                inplace=True,
            )
        except Exception as e:
            logger.error(e)
            df["http_client"].fillna("curl_cffi", inplace=True)
            df["ads_page_url"].fillna("", inplace=True)

        logger.info(df)

        first_run_urls = [
            (
                row["domain"],
                defaultdict(None, {
                    "http_client": row["http_client"],
                    "inventory_type": row["inventory_type"],
                    "ads_txt_page_url": row["ads_page_url"],
                }),
            )
            for _, row in df.iterrows()
        ]

    # ── Run spiders sequentially using CrawlerRunner (single reactor) ────────
    configure_logging(settings)
    runner = CrawlerRunner(settings)

    @defer.inlineCallbacks
    def crawl_sequence():
        # ── 1. EXECUTE THE FIRST RUN SPIDER ──────────────────────────────────
        logger.info("🚀 Starting FIRST spider run (original domains)")
        yield runner.crawl(ScraperSpider, start_urls_with_meta=first_run_urls)
        logger.info("✅ First spider run complete")

        # ── 2. TARGET TODAY'S SPECIFIC EXCEL SESSION FILE ────────────────────
        second_run_urls = []
        logger.info("🔍 Checking for newly created ads_txt_inventory_partner Excel file...")
        
        # Pull the exact target path built by the current run settings block
        current_run_excel = settings.get("CUSTOM_RUN_SETTINGS")["COMMON"]["LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH"]

        if os.path.exists(current_run_excel):
            logger.info(f"📊 Found active session partner file: {current_run_excel}")
            df_ipd = pd.read_excel(current_run_excel)
        else:
            # 🎯 FIX: Avoid globally scanning for past dates when today's run yielded no IPD domains
            logger.warning(f"⚠️ No inventory partner file generated for today's session at: {current_run_excel}")
            df_ipd = pd.DataFrame()

        # Parse unique target domains out safely
        if not df_ipd.empty and "inventory_partner_domain" in df_ipd.columns:
            unique_ip_domains = df_ipd["inventory_partner_domain"].dropna().str.strip().unique()
            unique_ip_domains = [d for d in unique_ip_domains if d] # Clear blank lines
            logger.info(f"🎯 Found {len(unique_ip_domains)} unique inventory partner domains for this session.")
            
            second_run_urls = [
                (domain, defaultdict(None, {
                    "http_client": "curl_cffi",
                    "inventory_type": "ads.txt",
                    "ads_txt_page_url": "",
                })) for domain in unique_ip_domains
            ]
        else:
            logger.warning("⏭️ No inventory partner domains found for today's execution context.")

        # ── 3. EXECUTE THE SECOND RUN SPIDER (IF TARGETS FOUND) ──────────────
        if second_run_urls:
            # Generate secondary runtime suffix strings
            ipd_execution_date = datetime.datetime.now(datetime.timezone.utc)
            ipd_suffix = ipd_execution_date.strftime("%Y-%m-%d-%H-%M-%S") + "_IPD"
            utc_date_str = ipd_execution_date.strftime("%Y-%m-%d")
            root_output = Path.cwd() / "data_output" / utc_date_str

            # Remap workspace configurations. The '_IPD' tag will trip DynamicToBqDict routing
            settings.get("CUSTOM_RUN_SETTINGS")["COMMON"].update({
                "LOCAL_SUCCESS_FILE_PATH":  root_output / f"ads_txt_success_{ipd_suffix}.parquet",
                "LOCAL_FAILURE_FILE_PATH":  root_output / f"ads_txt_failure_{ipd_suffix}.parquet",
                "LOCAL_METADATA_FILE_PATH": root_output / f"ads_txt_metadata_{ipd_suffix}.parquet",
                "LOCAL_INVENTORY_PARTNER_FILE_PATH":  root_output / f"ads_txt_inventory_partner_{ipd_suffix}.parquet",
                "LOCAL_INVENTORY_PARTNER_EXCEL_FILE_PATH": root_output / f"ads_txt_inventory_partner_{ipd_suffix}.xlsx",
            })

            settings.set("EXECUTION_DATE", ipd_execution_date)

            logger.info("🚀 Starting SECOND spider run (inventory partner domains only)")
            yield runner.crawl(ScraperSpider, start_urls_with_meta=second_run_urls)
            logger.info("✅ Second spider run complete")
        else:
            logger.info("⏭️ Skipping second crawl sequence: No domains found to parse for today.")

    # Execute the asynchronous sequence chains inside the event loop
    d = crawl_sequence()
    d.addErrback(lambda f: logger.error(f"❌ Crawl sequence error: {f}"))
    d.addBoth(lambda _: reactor.stop())

    reactor.run()
    logger.info("✅ All spider crawls finished")