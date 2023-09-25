import csv
import os
import sys
import re
from logger import logger
from pathlib import Path

import boto3
import botocore

# run this program with aws-okta:
# aws-okta exec prod-e2e-ops-support -- python3 download_pdf.py

# -------- update these variables -----------
# last_finished_line_num = 102282   # for re-run, 1-based line number
last_finished_line_num = 1   # for re-run, 1 based line number
target_root_folder = "/Users/jzhang/hli/report_pdfs"  # to store the downloaded files
# --------------------------------------------

# logger.set_level_to_debug()
# test_se_file = 's3://hli-datalake-nv-prod-us-west-2-prod/external/deliver/P1/S1/report/HSUB51A77993/HORD6C261109/radiology-summary-brain_36d6cdd5-b60f-4f8c-b5d2-2e43d650b75f_2020-2-27.pdf'


def parse_s3_uri(s3_uri):
    full_path = s3_uri[len("s3://"):]
    while full_path.endswith('/'):
        full_path.removesuffix('/')
    while full_path.startswith('/'):
        full_path.removeprefix('/')

    parts = full_path.split('/')
    bucket_name = parts.pop(0)
    file_name = parts.pop(-1)
    file_path = "/".join(parts)
    return (bucket_name, file_path, file_name)


def fix_file_name(s3_uri: str) -> tuple:
    """
    filenames on S3 are case sensitiv. the S3 links from output.csv are all in lower cases.
    we know that patner id, site id, subject id, order id should all in upper case.
    The spelling of the file name are unknown
    to fix this, list S3 file names under the folder and test they are the same by comparing in lower cases.
    if they are matched, use the one from S3 as the correct file name.

    For example:
    's3://hli-datalake-nv-prod-us-west-2-prod/external/deliver/p1/s1/report/hsub51a77993/hord6c261109/radiology-summary-brain_36d6cdd5-b60f-4f8c-b5d2-2e43d650b75f_2020-2-27.pdf'
    will be fixed as this:
    's3://hli-datalake-nv-prod-us-west-2-prod/external/deliver/P1/S1/report/HSUB51A77993/HORD6C261109/radiology-summary-brain_36d6cdd5-b60f-4f8c-b5d2-2e43d650b75f_2020-2-27.pdf'
    """

    bucket_name, file_folder, file_name = parse_s3_uri(s3_uri)
    contents = []
    path_exists = False

    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=file_folder
        )
        contents = response.get('Contents', [])
        if contents:
            path_exists = True
    except Exception as e:
        logger.error(f"list files Error: {e}")

    if not contents:
        logger.warning(f"Cannot find S3 path {bucket_name}/{file_folder}, skipping")

    for content in contents:
        key = content['Key']
        s3_file_name = key.split('/')[-1]
        if s3_file_name.lower() == file_name:
            file_name = s3_file_name
            break
    return (bucket_name, file_folder, file_name, path_exists)


def fix_s3_path(s3_path: str) -> str:
    # fix partener code
    partner_p = r'/p\d/'
    m = re.search(partner_p, s3_path)
    if m:
        s3_path = s3_path.replace(m.group(), m.group().upper())
    # fix site code
    partner_s = r'/s\d/'
    m = re.search(partner_s, s3_path)
    if m:
        s3_path = s3_path.replace(m.group(), m.group().upper())

    # fix subject id
    partner_hsub = r'/hsub\w+/'
    m = re.search(partner_hsub, s3_path)
    if m:
        s3_path = s3_path.replace(m.group(), m.group().upper())

    # fix order id
    partner_hord = r'/hord\w+/'
    m = re.search(partner_hord, s3_path)
    if m:
        s3_path = s3_path.replace(m.group(), m.group().upper())

    # fix file name, compare from listed with the one in the key
    return fix_file_name(s3_path)


def download_pdf_from_aws(full_s3_path: str, cur_line_num: int, overwrite=False) -> None:
    logger.debug(f"input full s3 path = {full_s3_path}")

    bucket_name, file_folder, file_name, s3_path_exists = fix_s3_path(full_s3_path)
    if not s3_path_exists:
        return

    object_key = '/'.join([file_folder, file_name])
    save_to_file = '/'.join([target_root_folder, bucket_name, object_key])
    logger.debug(f"bucket name: {bucket_name}")
    logger.debug(f"object_key: {object_key}")
    logger.debug(f"save to path: {save_to_file}")

    if not overwrite and os.path.isfile(save_to_file):
        return

    target_file_path = Path(save_to_file)
    target_folder = target_file_path.parent
    target_folder.mkdir(parents=True, exist_ok=True)

    try:
        s3_client = boto3.client('s3')
        logger.info(f"Downloading s3://{bucket_name}/{object_key} and saved to {save_to_file}")
        s3_client.download_file(bucket_name, object_key, save_to_file)
        logger.info(f"Line {cur_line_num} Processed: {object_key}")

    except botocore.exceptions.ClientError as e:
        err_coce = e.response['Error']['Code']
        if err_coce == '403':
            logger.fatal("Please run this program with aws-okta. see the beginning of this file")
            sys.exit(1)
        if err_coce == '404':
            logger.warning(f"Source file not found: {full_s3_path}. Is the file stores in Glacier?")
        else:
            logger.error(f"exception {e}")
        logger.info(f"Line {cur_line_num} skipped. Cannot download souce file")

    except Exception as e:
        logger.debug("got exception when downloading file " + str(type(e)))
        logger.error(f"Failed to download file at line {cur_line_num}, to {save_to_file}: {e}")
        logger.info(f"Cannot download file. Skipping source line {cur_line_num}")


def main(finished_line_num: int) -> None:
    with open("output.csv", 'rt') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)  # skip the headers
        count = 1
        # skip finished lines
        for row in csv_reader:
            if count <= finished_line_num:
                count += 1
                continue

            finished_line_num += 1
            full_s3_pdf_path = row[-1]
            download_pdf_from_aws(full_s3_pdf_path, finished_line_num)


if __name__ == '__main__':
    main(last_finished_line_num)
