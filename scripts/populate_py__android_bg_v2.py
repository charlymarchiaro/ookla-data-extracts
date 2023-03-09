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
    filemode="a",
    format="%(asctime)s - %(message)s",
)

load_dotenv(consts.env_file_path)


files = [
    'C:\\Users\\Usuario\\Desktop\\android_bg_v2_2023-03-05.csv',
    'C:\\Users\\Usuario\\Desktop\\android_bg_v2_2023-03-06.csv',
    'C:\\Users\\Usuario\\Desktop\\android_bg_v2_2023-03-07.csv',
]

for file in files:
    db_handler.save_android_bg_v2_file(file, 'public.py__android_bg_v2')
