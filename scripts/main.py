import os
import sys
from datetime import datetime, timedelta

import logging

import scripts.consts as consts
import scripts.db_handler as db_handler
import scripts.ookla_api_handler as ookla_api_handler

from dotenv import load_dotenv

logging.basicConfig(
    filename=consts.log_file_path,
    level=logging.DEBUG,
    filemode='a',
    format="%(asctime)s - %(message)s",
)

load_dotenv(consts.env_file_path)


def update_android_bg_v2_data():
    # Get list of all currently stored dates
    stored_dates = db_handler.get_android_bg_v2_stored_dates()
    stored_dates_str = [d.strftime("%Y-%m-%d") for d in stored_dates]

    # Get list af all dates that should be stored
    dates_str = [
        (datetime.today() - timedelta(i + 1)).strftime("%Y-%m-%d")
        for i in range(0, consts.total_stored_days)
    ]

    # Get the list of all missing dates
    missing_dates_str = []
    for date_str in dates_str:
        if date_str not in stored_dates_str:
            missing_dates_str.append(date_str)

    missing_files = ookla_api_handler.select_remote_android_bg_v2_files(
        missing_dates_str
    )
    if len(missing_files) == 0:
        logging.info("The local database is up to date.")
        sys.exit()

    # Download and save all files
    for file in missing_files:
        try:
            file_path = ookla_api_handler.download_and_unzip_file(file)
            db_handler.save_android_bg_v2_file(file_path)
            os.remove(file_path)

        except Exception as error:
            logging.error(error)


update_android_bg_v2_data()
