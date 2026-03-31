# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import os

# import datetime
from pathlib import Path

# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter
from scrapy.spiders import Spider
import pandas as pd

# import fastparquet as fp
from scrapy.utils.project import get_project_settings

from ads_txt.items import AdsTxtItem, AdsTxtErrorItem, AdsTxtMetadataItem
from ads_txt.utils import GoogleServices, remove_dir


class AdsTxtPipeline:
    project_settings = get_project_settings()
    custom_run_settings: dict = project_settings.get("CUSTOM_RUN_SETTINGS")
    common_settings: dict = custom_run_settings.get("COMMON")

    cloud_settings: dict = custom_run_settings.get("CLOUD_SAVE")
    to_gcs_settings: dict = cloud_settings.get("TO_GCS")
    to_bq_settings: dict = cloud_settings.get("TO_BQ")

    # Common

    # UTC_DATETIME:datetime.datetime = project_settings.get("EXECUTION_DATE")
    LOG_FILE_PATH: Path = common_settings.get("LOG_FILE_PATH")
    BATCH_SIZE: int = common_settings.get(
        "BATCH_SIZE"
    )  # ✅ Save data every 100 items (adjust as needed)
    ROOT_DATA_OUTPUT: Path = common_settings.get("ROOT_DATA_OUTPUT")
    ROOT_DATA_OUTPUT_L1D: Path = common_settings.get("ROOT_DATA_OUTPUT_L1D")

    REMOVE_SAME_DAY_OUTPUT_FILES: bool = common_settings.get(
        "REMOVE_SAME_DAY_OUTPUT_FILES"
    )
    REMOVE_YESTERDAY_OUTPUT_FILES: bool = common_settings.get(
        "REMOVE_YESTERDAY_OUTPUT_FILES"
    )

    LOCAL_SUCCESS_FILE_PATH: Path = common_settings.get("LOCAL_SUCCESS_FILE_PATH")
    LOCAL_FAILURE_FILE_PATH: Path = common_settings.get("LOCAL_FAILURE_FILE_PATH")
    LOCAL_METADATA_FILE_PATH: Path = common_settings.get("LOCAL_METADATA_FILE_PATH")

    # cloud save settings
    PROJECT_ID: str = cloud_settings.get("PROJECT_ID")
    SERVICE_ACCOUNT_JSON_PATH: Path = cloud_settings.get("SERVICE_ACCOUNT_JSON_PATH")

    # To GCS
    UPLOAD_TO_GCS: bool = to_gcs_settings.get("UPLOAD_TO_GCS")
    GCS_BUCKET: str = to_gcs_settings.get("GCS_BUCKET")
    GCS_SUCCESS_GRI: str = to_gcs_settings.get("GCS_SUCCESS_GRI")
    GCS_FAILURE_GRI: str = to_gcs_settings.get("GCS_FAILURE_GRI")
    GCS_METADATA_GRI: str = to_gcs_settings.get("GCS_METADATA_GRI")

    # To BQ
    UPLOAD_GCS_TO_BQ: bool = to_bq_settings.get("UPLOAD_GCS_TO_BQ")
    BQ_DATASET_ID: str = to_bq_settings.get("BQ_DATASET_ID")
    ADS_TXT_SUCCESS_BQ_TABLE_ID: str = to_bq_settings.get("ADS_TXT_SUCCESS_BQ_TABLE_ID")
    ADS_TXT_FAILURE_BQ_TABLE_ID: str = to_bq_settings.get("ADS_TXT_FAILURE_BQ_TABLE_ID")
    ADS_TXT_METADATA_BQ_TABLE_ID: str = to_bq_settings.get(
        "ADS_TXT_METADATA_BQ_TABLE_ID"
    )

    def open_spider(self, spider: Spider):
        """Initialize empty lists for batching before writing to Parquet"""
        self.success_data = []
        self.failure_data = []
        self.metadata_data = []

        # Ensure folders exist and are not conflicting with single files
        # for folder in [self.success_file, self.failure_file, self.metadata_file]:
        spider.logger.info(f"{self.REMOVE_YESTERDAY_OUTPUT_FILES=}")
        if self.REMOVE_YESTERDAY_OUTPUT_FILES:
            remove_dir(self.ROOT_DATA_OUTPUT_L1D)

        self.ensure_directory(self.ROOT_DATA_OUTPUT)
        self.ensure_directory(self.LOG_FILE_PATH.parent)

    def process_item(self, item, spider: Spider):
        """Process each Scrapy item and store it in the correct list"""
        if isinstance(item, AdsTxtItem):
            ads_txt_item = dict(item)
            ads_txt_line: str = ads_txt_item.get(
                "ads_txt_line", "ads_txt_pipeline_issue"
            )
            ads_txt_item["ads_txt_line"] = ads_txt_line.splitlines()
            ads_txt_item_df = pd.DataFrame(ads_txt_item)

            self.success_data.append(ads_txt_item_df)
        elif isinstance(item, AdsTxtErrorItem):
            self.failure_data.append(dict(item))
        elif isinstance(item, AdsTxtMetadataItem):
            self.metadata_data.append(dict(item))

        # ✅ Save data periodically to prevent memory issues
        if len(self.success_data) >= self.BATCH_SIZE:
            self.append_to_parquet(
                self.success_data, self.LOCAL_SUCCESS_FILE_PATH, df_concat=True
            )
            self.success_data = []  # Clear buffer after saving

        if len(self.failure_data) >= self.BATCH_SIZE:
            self.append_to_parquet(self.failure_data, self.LOCAL_FAILURE_FILE_PATH)
            self.failure_data = []

        if len(self.metadata_data) >= self.BATCH_SIZE:
            self.append_to_parquet(self.metadata_data, self.LOCAL_METADATA_FILE_PATH)
            self.metadata_data = []

        return item

    # def close_spider(self, spider: Spider):
    #     """Write remaining data to Parquet when the spider finishes"""
    #     spider.logger.critical(spider.crawler.stats.get_stats())

    #     try:
    #         gservice = GoogleServices(self.PROJECT_ID, self.SERVICE_ACCOUNT_JSON_PATH)

    #         if self.success_data:
    #             self.append_to_parquet(
    #                 self.success_data, self.LOCAL_SUCCESS_FILE_PATH, df_concat=True
    #             )

    #         if self.failure_data:
    #             self.append_to_parquet(self.failure_data, self.LOCAL_FAILURE_FILE_PATH)

    #         if self.metadata_data:
    #             self.append_to_parquet(
    #                 self.metadata_data, self.LOCAL_METADATA_FILE_PATH
    #             )

    #         spider.logger.info(f"{self.UPLOAD_TO_GCS=}")
    #         if self.UPLOAD_TO_GCS:
    #             # if False:
    #             ads_txt_success_uri = gservice.upload_parquet_to_gcs(
    #                 self.LOCAL_SUCCESS_FILE_PATH, self.GCS_BUCKET, self.GCS_SUCCESS_GRI
    #             )
    #             ads_txt_failure_uri = gservice.upload_parquet_to_gcs(
    #                 self.LOCAL_FAILURE_FILE_PATH, self.GCS_BUCKET, self.GCS_FAILURE_GRI
    #             )
    #             ads_txt_metadata_uri = gservice.upload_parquet_to_gcs(
    #                 self.LOCAL_METADATA_FILE_PATH,
    #                 self.GCS_BUCKET,
    #                 self.GCS_METADATA_GRI,
    #             )

    #             spider.logger.info(f"{self.UPLOAD_GCS_TO_BQ=}")
    #             if self.UPLOAD_GCS_TO_BQ:
    #                 # if False:
    #                 dataset_id = self.BQ_DATASET_ID
    #                 if ads_txt_success_uri:
    #                     gservice.replace_gcs_parquet_to_bq(
    #                         ads_txt_success_uri,
    #                         dataset_id,
    #                         self.ADS_TXT_SUCCESS_BQ_TABLE_ID,
    #                     )
    #                 if ads_txt_failure_uri:
    #                     gservice.replace_gcs_parquet_to_bq(
    #                         ads_txt_failure_uri,
    #                         dataset_id,
    #                         self.ADS_TXT_FAILURE_BQ_TABLE_ID,
    #                     )
    #                 if ads_txt_metadata_uri:
    #                     gservice.replace_gcs_parquet_to_bq(
    #                         ads_txt_metadata_uri,
    #                         dataset_id,
    #                         self.ADS_TXT_METADATA_BQ_TABLE_ID,
    #                     )

    #         spider.logger.info(f"{self.REMOVE_SAME_DAY_OUTPUT_FILES=}")
    #         if self.REMOVE_SAME_DAY_OUTPUT_FILES:
    #             remove_dir(self.ROOT_DATA_OUTPUT)

    #     finally:
    #         gservice.service_close()

    def close_spider(self, spider: Spider):
        spider.logger.critical(spider.crawler.stats.get_stats())

        # Always flush remaining local data first (no cloud dependency)
        if self.success_data:
            self.append_to_parquet(self.success_data, self.LOCAL_SUCCESS_FILE_PATH, df_concat=True)
        if self.failure_data:
            self.append_to_parquet(self.failure_data, self.LOCAL_FAILURE_FILE_PATH)
        if self.metadata_data:
            self.append_to_parquet(self.metadata_data, self.LOCAL_METADATA_FILE_PATH)

        gservice = None
        try:
            if self.UPLOAD_TO_GCS or self.UPLOAD_GCS_TO_BQ:
                gservice = GoogleServices(self.PROJECT_ID, self.SERVICE_ACCOUNT_JSON_PATH)

                spider.logger.info(f"{self.UPLOAD_TO_GCS=}")
                if self.UPLOAD_TO_GCS:
                    ads_txt_success_uri = gservice.upload_parquet_to_gcs(
                        self.LOCAL_SUCCESS_FILE_PATH, self.GCS_BUCKET, self.GCS_SUCCESS_GRI
                    )
                    ads_txt_failure_uri = gservice.upload_parquet_to_gcs(
                        self.LOCAL_FAILURE_FILE_PATH, self.GCS_BUCKET, self.GCS_FAILURE_GRI
                    )
                    ads_txt_metadata_uri = gservice.upload_parquet_to_gcs(
                        self.LOCAL_METADATA_FILE_PATH, self.GCS_BUCKET, self.GCS_METADATA_GRI
                    )

                    spider.logger.info(f"{self.UPLOAD_GCS_TO_BQ=}")
                    if self.UPLOAD_GCS_TO_BQ:
                        dataset_id = self.BQ_DATASET_ID
                        if ads_txt_success_uri:
                            gservice.replace_gcs_parquet_to_bq(ads_txt_success_uri, dataset_id, self.ADS_TXT_SUCCESS_BQ_TABLE_ID)
                        if ads_txt_failure_uri:
                            gservice.replace_gcs_parquet_to_bq(ads_txt_failure_uri, dataset_id, self.ADS_TXT_FAILURE_BQ_TABLE_ID)
                        if ads_txt_metadata_uri:
                            gservice.replace_gcs_parquet_to_bq(ads_txt_metadata_uri, dataset_id, self.ADS_TXT_METADATA_BQ_TABLE_ID)

            spider.logger.info(f"{self.REMOVE_SAME_DAY_OUTPUT_FILES=}")
            if self.REMOVE_SAME_DAY_OUTPUT_FILES:
                remove_dir(self.ROOT_DATA_OUTPUT)

        except Exception as e:
            spider.logger.error(f"Error in pipeline close_spider: {e}", exc_info=True)

        finally:
            if gservice:
                gservice.service_close()

    def ensure_directory(self, folder):
        """Ensure the folder exists and is not a conflicting file"""

        if os.path.exists(folder) and not os.path.isdir(folder):
            # os.remove(folder) # Delete conflicting file
            # os.makedirs(folder)
            pass

        elif not os.path.exists(folder):
            os.makedirs(folder)

    def append_to_parquet(self, data, file_path, df_concat: bool = False):
        """Append data efficiently using fastparquet.to_parquet()"""
        if df_concat:
            df = pd.concat(data, ignore_index=True)
        else:
            df = pd.DataFrame(data)

        if not df.empty:
            if os.path.exists(file_path):
                df.to_parquet(file_path, engine="fastparquet", append=True)
                # Save XLSX in parallel
                self.write_excel(df, file_path)
            else:
                df.to_parquet(file_path, engine="fastparquet")
                # Save XLSX in parallel
                self.write_excel(df, file_path)

    def write_excel(self, df: pd.DataFrame, file_path: Path):
        excel_path = file_path.with_suffix(".xlsx")
        try:
            df.to_excel(excel_path, index=False, engine="openpyxl")
            return excel_path
        except Exception as e:
        
            return None
