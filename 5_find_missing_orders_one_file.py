import csv
import json
import requests
import time
import traceback

from logger import logger
# from prism_client.api.client import PrismApiClient
# from datalake_client import DatalakeClient


from urllib3.exceptions import InsecureRequestWarning
# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


# Keep update this value with last finished index id. this is 1-based
finished_line_num = 0


# Update with valid token
prod_token = "56d831ce-42b1-d8b2-e86a-8e51ead43283"
# ----------------------------------------------------

# Update to your local path for these files
subject_csv = "prism_public_subject.csv"
# subject_csv = "test_subjects.csv"
active_members_csv = "ActMembers091323.csv"

out_csv = "order_list.csv"
out_csv_missing = "order_missing_list.csv"
# -----------------------------------------------

retry_limit = 10
prod_arn = 'hrn:auth:user:prod-e2e-ops-support'

prism_url = 'https://prism-api.hli.io'
datalake_url = 'https://datalake-api.hli.io'


headers = {
    'Authorization': prod_token,
    'hrn': prod_arn,
    'Content-Type': 'application/json'
}

# logger.set_level_to_debug()
start_time = time.time()

# roder status
# exlude cancelled get_order--> prism


def get_subject_order_list(hli_subject_id: str):
    url = f"{prism_url}/api/subject/{hli_subject_id}/orders"
    payload = {}
    output = []
    for i in range(retry_limit):
        try:
            response = requests.request("GET", url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                for item in json.loads(response.text):
                    output.append(item)
                return output

            logger.info(f"get_subject_order_list() with hli_subject_id {hli_subject_id} got response with code {response.status_code}")
            if response.status_code == 404:
                return output

            elif response.status_code == 403:
                logger.info(f"get_subject_order_list() returned: {response.text}")
                logger.error("Is the access token expired?")

        except (Exception) as e:
            logger.error(e)
            traceback.print_exc()

        delay = 5 * (i + 1)
        logger.info(f"get_subject_order_list() Retry {i} with delay {delay} seconds")
        time.sleep(delay)

    raise Exception("get_subject_order_list() timeout at {delay} seconds")


def find_order_dataset(hli_order_reference):
    url = f"{datalake_url}/datasets/report/search"
    payload = json.dumps({
        "hli_order_reference": [hli_order_reference]
    })
    for i in range(retry_limit):
        try:
            response = requests.request("POST", url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.text

            logger.info(f"find_order_dataset() got response with code {response.status_code}")
            if response.status_code == 403:
                logger.info(f"find_order_dataset() returned: {response.text}")
                logger.error("Is the access token expired?")

        except (Exception) as e:
            logger.error(e)
            # traceback.print_exc()

        delay = 3 * (i + 1)
        logger.info(f"find_order_dataset() Retry {i} with delay {delay} seconds")
        time.sleep(delay)

    raise Exception("find_order_dataset() timeout at {delay} seconds")
    # return '{}'


def search_datasets(hli_subject_id):
    url = f"{datalake_url}/datasets/report/search?limit=65"

    payload = json.dumps({
        "hli_subject_id": [
            hli_subject_id
        ]
    })

    for i in range(retry_limit):
        try:
            response = requests.request("POST", url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.text

            logger.debug(f"search_datasets() got response with code {response.status_code}")
            if response.status_code == 403:
                logger.debug(f"search_datasets() returned {response.text}")
                logger.error("Is the access token expired?")

        except (Exception) as e:
            logger.error(e)
            # traceback.print_exc()

        delay = 2 * (i + 1)
        logger.info(f"search_datasets() Retry {i} with delay {delay} seconds")
        time.sleep(delay)

    raise Exception("search_datasets() timeout ")
    # return '{}'


def get_order_product_name(order_product_code):
    url = f"{prism_url}/api/product?product_code={order_product_code}"
    payload = {}
    for i in range(retry_limit):
        try:
            response = requests.request("GET", url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.text

            logger.info(f"get_order_product_name() got response with code {response.status_code}")
            if response.status_code == 403:
                logger.error(f"get_order_product_name() returned: {response.text}")
                logger.info("Is the access token expired?")

        except (Exception) as e:
            logger.error(e)
            # traceback.print_exc()

        delay = 5 * (i + 1)
        logger.info(f"get_order_product_name() Retry {i} with delay {delay} seconds")
        time.sleep(delay)

    raise Exception("get_order_product_name() timeout ")


def get_subject_info(client_subject_id: str) -> str:
    url = f"{prism_url}/api/subject?client_subject_id={client_subject_id}"
    payload = {}
    for i in range(retry_limit):
        try:
            response = requests.request("GET", url, headers=headers, data=payload, verify=False)
            if response.status_code == 200:
                return response.text

            logger.info(f"get_subject_info() got response with code {response.status_code}")

            if response.status_code == 403:
                logger.error(f"get_subject_info() returned: {response.text}")
                logger.info("Is the access token expired?")

            elif response.status_code == 404:
                logger.info(f"Invalid client_subject_id: {client_subject_id}, no data")
                return '{}'

        except (Exception) as e:
            logger.error(e)
            # traceback.print_exc()

        delay = 5 * (i + 1)
        logger.info(f"get_subject_info() Retry {i} with delay {delay} seconds")
        time.sleep(delay)

    raise Exception("get_subject_info() timeout ")


active_members_set = set()


def read_active_member_list():
    # [Primary Contact,Person Number,Contact: Email,Membership Start Date,Membership Expiration Date,Date of Birth,100+ Physician]
    with open(active_members_csv, "rt", newline='', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader, None)
        for row in csv_reader:
            if not row[1]:
                logger.warning(f"while reading active member list, {row[0]} doesn't have valid PN, skipping.")
            else:
                pn = row[1]
                active_members_set.add(pn)

        logger.info(f"{len(active_members_set)} valid active members loaded")


output_field_names = ['hli_subject_id', 'client_subject_id', 'client_subject_id_type', 'dob', 'email', 'first_name', 'last_name', 'latest_order_date', 'sex', 'dataset_id', 'hli_order_reference', 'created_on', 'order_date', 'order_status', 'order_metadata', 'order_product_versions', 'report_name', 'report_path']
# PN, HLI Order Ref, Order Date, HLI Order -  Product code, HLI Order - Metadata (product name), Report name, Report path


def write_headers():
    # write headers will overwrite existing data
    with open(out_csv, 'wt', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_field_names)
        writer.writeheader()

    with open(out_csv_missing, 'wt', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_field_names)
        writer.writeheader()


def save_results(output, output_missing):
    with open(out_csv, 'at', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_field_names)
        writer.writerows(output)

    with open(out_csv_missing, 'at', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_field_names)
        writer.writerows(output_missing)


product_code2name = dict()


def create_one_result(hli_subject_id, subject_info: dict, order_reference: dict, dataset_id, report_name, report_path) -> dict:
    """
    ['hli_subject_id', 'client_subject_id', 'client_subject_id_type', 'dob', 'email', 'first_name', 'last_name', 'latest_order_date', 'sex', 'dataset_id',
     'hli_order_reference', 'created_on', 'order_date', 'order_status', 'order_metadata', 'order_product_versions',
     'report_name', 'report_path']
    """
    if not subject_info:
        raise Exception("create_one_result(): subject_info is a required parameter of dict type")

    result = {
        "hli_subject_id": hli_subject_id,
        "client_subject_id": subject_info.get("client_subject_id", ""),
        "client_subject_id_type": subject_info.get("client_subject_id_type", ""),
        "dob": subject_info.get("dob", ""),
        "email": subject_info.get("email", ""),
        "first_name": subject_info.get("first_name", ""),
        "last_name": subject_info.get("last_name", ""),
        "latest_order_date": subject_info.get("latest_order_date", ""),
        "sex": subject_info.get("sex"),

        "dataset_id": dataset_id,

        "hli_order_reference": order_reference.get('hli_order_reference', ""),
        "created_on": order_reference.get("created_on", ""),
        "order_date": order_reference.get("order_date", ""),
        "order_status": order_reference.get('order_status', ""),
        "order_metadata": order_reference.get('order_metadata', {}),
        "order_product_versions": order_reference.get('product_versions', {}),

        "report_name": report_name,
        "report_path": report_path
    }

    return result


def process_one_row(row):
    output = []
    output_missing = []

    line_num = row[0]
    hli_subject_id = row[1]
    client_subject_id = row[2]

    try:
        subject_info_items = json.loads(get_subject_info(client_subject_id))
        if not subject_info_items or not subject_info_items[0]:
            logger.error(f"Cannot find client_subject_id {client_subject_id}")
            return ([], [])

        subject_info = subject_info_items[0]

        orders = get_subject_order_list(hli_subject_id)
        if not orders:
            logger.info(f"{line_num :4s} subject_id {hli_subject_id} done, no order found")
            # create_one_result(hli_subject_id, subject_info, order_reference, dataset_id, report_name, report_path)
            result = create_one_result(hli_subject_id, subject_info, {}, '', '', '')
            if client_subject_id in active_members_set:
                output_missing.append(result)
            output.append(result)
            return (output, output_missing)

        for order_ref in orders:
            hli_order_reference = order_ref["hli_order_reference"]
            datasets = json.loads(find_order_dataset(hli_order_reference))
            if not datasets or datasets['count'] < 1:
                # this order reference is not part of any dataset. save it as missing
                logger.info(f"{row[0] :4s} subject_id {row[1]}, order_ref {hli_order_reference}, no dataset found")
                dataset_id = ""
                report_name = ""
                s3_pdf_path = ""
                result = create_one_result(hli_subject_id, subject_info, order_ref, dataset_id, report_name, s3_pdf_path)
                if client_subject_id in active_members_set:
                    output_missing.append(result)
                else:
                    logger.info(f"     -- PN {client_subject_id} is not an active member")

                output.append(result)

            else:
                # has dataset, output to the correct list
                datasets_count = datasets['count']
                logger.info(f"{row[0] :4s} subject_id {row[1]}, order_ref {hli_order_reference}, {datasets_count} dataset(s) found")

                for dataset in datasets['datasets']:
                    report_name = dataset['metadata']['report_name']
                    s3_pdf_path = dataset['path']
                    dataset_id = dataset['id']
                    result = create_one_result(hli_subject_id, subject_info, order_ref, dataset_id, report_name, s3_pdf_path)
                    output.append(result)

                # logger.info(f"{row[0] :4s} subject_id {row[1]} done with {datasets_count} dataset(s)")

            output_size = len(output)
            if output_size > 0:
                save_results(output, output_missing)
                output = []
                output_missing = []

    except (Exception) as e:
        logger.error(e)
        traceback.print_exc()

    return (output, output_missing)


def read_csv(finshed_line_number):
    output = []
    output_missing = []

    read_active_member_list()

    if finshed_line_number == 0:
        write_headers()

    # processed_count = 0
    with open(subject_csv, "rt", newline='', encoding='utf-8') as csv_file:

        csv_reader = csv.reader(csv_file, delimiter=',')
        if finshed_line_number not in (0, 1):
            logger.info(f"Skipping to input line {finshed_line_number + 1}")

        for row in csv_reader:

            cur_line_num = int(row[0])
            if (time.time() - start_time) / 3600 > 12:
                logger.error(f"has been running for more than 12 hours. Please update the access token and re-run this script with updated 'finished_line_num' to {cur_line_num - 1}.")
                return (output, output_missing)

            if cur_line_num <= finshed_line_number:
                continue

            # processed_count += 1
            results, results_missing = process_one_row(row)
            result_size = len(results)
            if result_size > 0:
                logger.info(f"     -- Saving {result_size} processed items")
            save_results(results, results_missing)
            results = []
            results_missing = []

        return results, results_missing


output_full, output_missing = read_csv(finished_line_num)
save_results(output_full, output_missing)
