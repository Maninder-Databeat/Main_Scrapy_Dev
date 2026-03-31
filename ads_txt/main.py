import datetime
import os
from collections import defaultdict
# from pprint import pprint

import certifi
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from dotenv import load_dotenv
# import pandas as pd

from ads_txt.utils import GoogleServices
from loguru_loggins import get_logger


load_dotenv(r".secrets/.env")
logger = get_logger()

# Manually set the Scrapy project environment
os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "ads_txt.settings")
os.environ["SSL_CERT_FILE"] = certifi.where()
# print(f"{certifi.where()=}")

from ads_txt.spiders.scraper_spider import ScraperSpider


def run_spider(start_urls_with_meta, project_setting, is_test=True):
    # Override settings dynamically (optional)
    # project_setting.set("LOG_LEVEL", "INFO")
    # project_setting.set("FEEDS", {"output.json": {"format": "json"}})

    # start_urls = ["https://tls.browserleaks.com/json"]
    if is_test:
        project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_GCS"][
            "UPLOAD_TO_GCS"
        ] = False
        project_setting.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["TO_BQ"][
            "UPLOAD_GCS_TO_BQ"
        ] = False
        start_urls_with_meta = [
            (
                "yahoo.com",
                defaultdict(
                    None,
                    {
                        "http_client": "curl_cffi",
                        "inventory_type": "app-ads.txt",
                        "ads_txt_page_url": "",
                    },
                ),
            ),
            # (
            #     "www.citationmachine.net",
            #     defaultdict(
            #         None,
            #         {
            #             "http_client": "curl_cffi",
            #             "inventory_type": "ads.txt",
            #             "ads_txt_page_url": "https://www.citationmachine.net/a.txt",
            #         },
            #     ),
            # ),
            # (
            #     "thegazette.com",
            #     defaultdict(
            #         None,
            #         {
            #             "http_client": "default",
            #             "inventory_type": "app-ads.txt",
            #             "ads_txt_page_url": "",
            #         },
            #     ),
            # ),
            # (
            #     "notfound.com",
            #     defaultdict(
            #         None,
            #         {
            #             "http_client": "default",
            #             "inventory_type": "app-ads.txt",
            #             "ads_txt_page_url": "",
            #         },
            #     ),
            # ),
            # (
            #     "notfoundssss.com",
            #     defaultdict(
            #         None,
            #         {
            #             "http_client": "default",
            #             "inventory_type": "app-ads.txt",
            #             "ads_txt_page_url": "",
            #         },
            #     ),
            # ),
        ]

    # Start the Scrapy crawler
    process = CrawlerProcess(project_setting)
    process.crawl(ScraperSpider, start_urls_with_meta=start_urls_with_meta)
    process.start()


# if __name__ == "__main__":


#     logger.info("Spider crawl started")
#     # Now get_project_settings() will load settings from settings.py
#     settings = get_project_settings()

#     execution_date: str = datetime.datetime.now(datetime.timezone.utc)

#     settings.set("EXECUTION_DATE", execution_date)

#     logger.info(settings.copy_to_dict())

#     project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
#     service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"][
#         "SERVICE_ACCOUNT_JSON_PATH"
#     ]

#     try:
#         gservice = GoogleServices(project_id, service_account_file_path)

#         query = """ SELECT domain, inventory_type, ads_page_url, http_client FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table` """
#         df = gservice.query_bq_to_pd(query)

#         # If google sheet input is required. (Do Not change this file, make copy of this one)
#         # google_sheet_id = "1DLevo_k0LTfIHH7wL9n08NfYnpq_CbSF5RLNIMaIJU8"
#         # worksheet_name = "main_file"

#         # df = gservice.adhoc_gsheet_pull(google_sheet_id, worksheet_name)

#         df.drop(
#             columns=[
#                 col_name
#                 for col_name in df.columns
#                 if col_name
#                 not in ["http_client", "ads_page_url", "domain", "inventory_type"]
#             ],
#             inplace=True,
#         )

#         df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
        
#     finally:
#         gservice.service_close()

#     logger.info(df)
#     logger.info(df.dtypes)

#     # df = df.head(10)
#     try:
#         df.fillna(
#             {
#                 "http_client": "curl_cffi",
#                 "ads_page_url": "",
#                 "domain": "NA",
#                 "inventory_type": "ads.txt",
#             },
#             inplace=True,
#         )
#         # df.fillna({"ads_page_url": ""}, inplace=True)
#         # df.fillna({"domain": "NA"}, inplace=True)
#     except Exception as e:
#         logger.error(e)
#         df["http_client"].fillna("curl_cffi", inplace=True)
#         df["ads_page_url"].fillna("", inplace=True)

#     logger.info(df)

#     # df = pd.read_csv(r"C:\Users\zaid\Downloads\sovrn_web.csv")
#     start_urls_with_meta = [
#         (
#             row["domain"],  # ✅ Use domain instead of index
#             defaultdict(
#                 None,
#                 {
#                     "http_client": row["http_client"],
#                     "inventory_type": row["inventory_type"],
#                     "ads_txt_page_url": row["ads_page_url"],
#                 },
#             ),
#         )
#         for _, row in df.iterrows()
#     ]
#     del df
#     run_spider(start_urls_with_meta, project_setting=settings, is_test=True)
#     logger.info("Spider crawl finished")


if __name__ == "__main__":
    is_test = True  # True=local/test path, False=production path

    logger.info("Spider crawl started")
    settings = get_project_settings()
    execution_date = datetime.datetime.now(datetime.timezone.utc)
    settings.set("EXECUTION_DATE", execution_date)
    logger.info(settings.copy_to_dict())

    start_urls_with_meta = []

    if not is_test:
        project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
        service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["SERVICE_ACCOUNT_JSON_PATH"]

        gservice = GoogleServices(project_id, service_account_file_path)
        try:
            query = """ SELECT domain, inventory_type, ads_page_url, http_client FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table` """
            df = gservice.query_bq_to_pd(query)

                    # If google sheet input is required. (Do Not change this file, make copy of this one)
                # google_sheet_id = "1DLevo_k0LTfIHH7wL9n08NfYnpq_CbSF5RLNIMaIJU8"
                # worksheet_name = "main_file"

                # df = gservice.adhoc_gsheet_pull(google_sheet_id, worksheet_name)

            df.drop(
                    columns=[
                        col_name
                        for col_name in df.columns
                        if col_name
                        not in ["http_client", "ads_page_url", "domain", "inventory_type"]
                    ],
                    inplace=True,
                )

            df.drop_duplicates(keep="first", inplace=True, ignore_index=True)
                
        finally:
            gservice.service_close()

        logger.info(df)
        logger.info(df.dtypes)

            # df = df.head(10)
        try:
            df.fillna(
                {
                    "http_client": "curl_cffi",
                    "ads_page_url": "",
                    "domain": "NA",
                    "inventory_type": "ads.txt",
                },
                inplace=True,
            )
            # df.fillna({"ads_page_url": ""}, inplace=True)
            # df.fillna({"domain": "NA"}, inplace=True)
        except Exception as e:
            logger.error(e)
            df["http_client"].fillna("curl_cffi", inplace=True)
            df["ads_page_url"].fillna("", inplace=True)

        logger.info(df)

        # df = pd.read_csv(r"C:\Users\zaid\Downloads\sovrn_web.csv")
        
        start_urls_with_meta = [
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
        # finally:gservice.service_close()

    else:
        logger.info("Test mode active: skipping GCS/BQ service-account section")

    run_spider(start_urls_with_meta, project_setting=settings, is_test=is_test)
    logger.info("Spider crawl finished")