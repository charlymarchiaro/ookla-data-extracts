import os

import logging

import urllib.request
import json
import base64
import sys
from zipfile import ZipFile

import scripts.consts as consts


def list_remote_android_bg_v2_files() -> list[any]:
    extracts_url = "https://intelligence.speedtest.net/extracts/bgv2/"

    # Please replace MyApiKey and MyApiSecret below with your organization's API key.
    username = os.environ.get("DATA_EXTRACTS_API_KEY_ID")
    password = os.environ.get("DATA_EXTRACTS_API_KEY_SECRET")

    opener = urllib.request.build_opener()
    urllib.request.install_opener(opener)
    opener.addheaders = [("Accept", "application/json")]

    # setup authentication
    login_credentials = "%s:%s" % (username, password)
    base64string = base64.b64encode(login_credentials.encode("utf-8")).decode("ascii")
    opener.addheaders = [("Authorization", "Basic %s" % base64string)]

    # makes request for files
    try:
        response = urllib.request.urlopen(extracts_url).read()
    except urllib.request.HTTPError as error:
        logging.error(error)
        sys.exit()

    try:
        items = json.loads(response)
        return items

    except ValueError as error:
        logging.error(error)
        sys.exit()


def select_remote_android_bg_v2_files(dates_str: list[str]) -> list[any]:
    selected_files: list[str] = []
    files = list_remote_android_bg_v2_files()
    for date_str in dates_str:
        file_name = f"android_bg_v2_{date_str}.zip"
        try:
            file = next((x for x in files if x["name"] == file_name))
            if file is not None:
                selected_files.append(file)
        except StopIteration as e:
            continue
    return selected_files


def download_and_unzip_file(file: any) -> str:
    logging.info("Downloading " + file["name"] + "...")
    response = urllib.request.urlopen(file["url"])
    zip_file_path = consts.tmp_folder + "/" + file["name"]
    with open(zip_file_path, "wb") as f:
        f.write(response.read())

    logging.info("Extracting...")
    csv_file_name = None
    with ZipFile(zip_file_path, "r") as zip:
        csv_file_name = zip.namelist()[0]
        zip.extractall(path=consts.tmp_folder)

    os.remove(zip_file_path)

    return consts.tmp_folder + "/" + csv_file_name
