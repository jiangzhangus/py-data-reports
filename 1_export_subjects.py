# https://trello.com/c/sDBKKiOZ/4-script-to-download-report-information-on-a-recurring-basis
# Report S3 path, client PN, Report name, partner, site, date

import csv
import psycopg2
from logger import logger

output_file = 'prism_public_subject.csv'

# prod db
db_name = "prism"
db_port = 5432
prism_db_host = 'prism-db-prod-cluster.cluster-ca5umrdsbmu0.us-west-2.rds.amazonaws.com'
prism_db_user = "sa"
prism_db_pass = "cYzxKpT6nice9420Twg3VPkp"

# PostgreSQL database connection parameters
db_params = {
    'dbname': db_name,
    'user': prism_db_user,
    'password': prism_db_pass,
    'host': prism_db_host,
    'port': db_port
}

query = "SELECT hli_subject_id, client_subject_id FROM subject"
users = []

try:
    with psycopg2.connect(**db_params) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            users = cursor.fetchall()

            field_names = ['line_number', 'hli_subject_id', 'client_subject_id']
            with open(output_file, 'w') as out_csv:
                csv_writer = csv.writer(out_csv)
                line_number = 0
                out_rows = []
                for row in users:
                    line_number += 1
                    row_with_num = [line_number]
                    row_with_num.extend([field for field in row])
                    out_rows.append(row_with_num)

                if out_rows:
                    csv_writer.writerows(out_rows)
                    logger.info("Wrote lines " + str(len(out_rows)))
                else:
                    logger.info("no data to write")

except psycopg2.Error as e:
    logger.error(f"failed to query data: {e}")
