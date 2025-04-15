import datetime
import os
from collections import defaultdict
import tempfile
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from itertools import islice

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
                "www.citationmachine.net",
                defaultdict(
                    None,
                    {
                        "http_client": "curl_cffi",
                        "inventory_type": "ads.txt",
                        "ads_txt_page_url": "https://www.citationmachine.net/a.txt",
                    },
                ),
            ),
            (
                "thegazette.com",
                defaultdict(
                    None,
                    {
                        "http_client": "default",
                        "inventory_type": "app-ads.txt",
                        "ads_txt_page_url": "",
                    },
                ),
            ),
            (
                "notfound.com",
                defaultdict(
                    None,
                    {
                        "http_client": "default",
                        "inventory_type": "app-ads.txt",
                        "ads_txt_page_url": "",
                    },
                ),
            ),
            (
                "notfoundssss.com",
                defaultdict(
                    None,
                    {
                        "http_client": "default",
                        "inventory_type": "app-ads.txt",
                        "ads_txt_page_url": "",
                    },
                ),
            ),
        ]

    # Start the Scrapy crawler
    process = CrawlerProcess(project_setting)
    process.crawl(ScraperSpider, start_urls_with_meta=start_urls_with_meta)
    process.start()


def generate_start_urls(df_chunk):
    for _, row in df_chunk.iterrows():
        yield (
            row["domain"],
            defaultdict(
                None,
                {
                    "http_client": row["http_client"],
                    "inventory_type": row["inventory_type"],
                    "ads_txt_page_url": row["ads_page_url"],
                },
            ),
        )


def read_parquet_in_batches(parquet_path, batch_size):
    parquet_file = pq.ParquetFile(parquet_path)
    print(parquet_file.num_row_groups)
    for rg in range(parquet_file.num_row_groups):
        table = parquet_file.read_row_group(rg)
        df_chunk = table.to_pandas()

        it = iter(df_chunk.iterrows())
        while True:
            batch = list(islice(it, batch_size))
            if not batch:
                break
            yield pd.DataFrame([row for _, row in batch])


if __name__ == "__main__":
    logger.info("Spider crawl started")
    # Now get_project_settings() will load settings from settings.py
    settings = get_project_settings()

    execution_date: str = datetime.datetime.now(datetime.timezone.utc)

    settings.set("EXECUTION_DATE", execution_date)

    logger.info(settings.copy_to_dict())

    project_id = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"]["PROJECT_ID"]
    service_account_file_path = settings.get("CUSTOM_RUN_SETTINGS")["CLOUD_SAVE"][
        "SERVICE_ACCOUNT_JSON_PATH"
    ]

    try:
        gservice = GoogleServices(project_id, service_account_file_path)

        # query = """ SELECT domain, inventory_type, ads_page_url, http_client FROM `ads-txt-validator.ads_txt_scraper_data.start_urls_table` """
        # df = gservice.query_bq_to_pd(query)

        df = gservice.adhoc_gsheet_pull()

    finally:
        gservice.service_close()

    logger.info(df)
    logger.info(df.dtypes)

    BATCH_SIZE = int(os.getenv("URL_BATCH"))

    with tempfile.TemporaryDirectory() as temp_dir:
        tmp_file_name = Path(temp_dir) / "domains_meta_data.parquet"

        logger.info(f"Saving Parquet temp file at {tmp_file_name}")

        # Fill and save DataFrame
        df.fillna(
            {
                "http_client": "curl_cffi",
                "ads_page_url": "",
                "domain": "NA",
                "inventory_type": "ads.txt",
            },
            inplace=True,
        )

        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].fillna("").astype(str)

        table = pa.Table.from_pandas(df)
        pq.write_table(table, tmp_file_name, row_group_size=BATCH_SIZE)

        del df, table  # free memory early

        # Read and crawl batches
        for batch_num, df_batch in enumerate(
            read_parquet_in_batches(tmp_file_name, BATCH_SIZE), start=1
        ):
            logger.info(f"Running batch {batch_num}, size: {len(df_batch)}")
            start_urls_with_meta = generate_start_urls(df_batch)
            start_urls_with_meta = list(start_urls_with_meta)
            # run_spider(start_urls_with_meta, project_setting=settings, is_test=False)
            print(
                f"-----------------------------------------batch running ------ {batch_num}"
            )
            logger.info(f"Batch {batch_num} finished.")

        logger.info("All batches completed.")

    logger.info("Spider crawl finished")
