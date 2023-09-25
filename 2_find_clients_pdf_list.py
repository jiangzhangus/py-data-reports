import requests

from logger import logger
# from prism_client.api.client import PrismApiClient
# from datalake_client import DatalakeClient
import csv
import json
import time

from urllib3.exceptions import InsecureRequestWarning
# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


# Keep update this value with last finished index id. this is 1-based
finished_line_num = 6926  # 2023 08 29, was 6912

# Update with valid token
prod_token = 'a5778b87-54b5-3595-4148-20684361e396'
# ----------------------------------------------------

# Update to your local path to these files
subject_csv = "./prism_public_subject.csv"
out_csv = "./output.csv"
# -----------------------------------------------

prod_arn = 'hrn:auth:user:prod-e2e-ops-support'

prism_url = 'https://prism-api.hli.io'
datalake_url = 'https://datalake-api.hli.io'

headers = {
    'Authorization': prod_token,
    'hrn': prod_arn,
    'Content-Type': 'application/json'
}


def get_order_details(hli_order_reference):
    url = f"{prism_url}/api/order/{hli_order_reference}/details"
    payload = {}
    response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    return response.text


def search_datasets(hli_subject_id):
    url = f"{datalake_url}/datasets/report/search?limit=65"

    payload = json.dumps({
        "hli_subject_id": [
            hli_subject_id
        ]
    })

    response = requests.request("POST", url, headers=headers, data=payload, verify=False)
    return response.text


def read_csv(line):
    output = []
    with open(subject_csv, "rt") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            cur_line_num = int(row[0])
            if cur_line_num <= line:
                print(f"skipping line: {cur_line_num}")
                continue

            hli_subject_id = row[1]
            try:
                response = json.loads(search_datasets(hli_subject_id))
                # logger.info(f"response: {response}")
                count_value = int(response.get('count'))
                if count_value > 0:
                    datasets = response['datasets']
                    for i in datasets:
                        time.sleep(1)
                        hli_order_reference = i['metadata']['hli_order_reference']
                        order_details = json.loads(get_order_details(hli_order_reference))
                        order_date = order_details.get('order_date', 'Not Found')
                        result = {
                            "client_subject_id": row[2],
                            "report_name": i['metadata']['report_name'],
                            "order_date": order_date,
                            "partner_code": i['metadata']['partner_id'],
                            "site_code": i['metadata']['site_id'],
                            "s3_pdf_path": i['path']
                        }
                        output.append(result)
                logger.info(f"{row[0]} with {row[1]} done")
            except Exception as e:
                logger.error(e)
                break
    return output


output = read_csv(finished_line_num)

field_names = ['client_subject_id', 'report_name', 'order_date', 'partner_code', 'site_code', 's3_pdf_path']
with open(out_csv, 'a') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    if finished_line_num == 0:
        writer.writeheader()
    writer.writerows(output)
