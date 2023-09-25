# Python Files:

* 1_export_subjects.py:
query prism for user/subject information and save into prism_public_subject.csv

* 2_find_client_pdf_list.py:
  - list all PDF files and related data into output.csv.
  - columns: client_subject_id,report_name,order_date,partner_code,site_code,s3_pdf_path


* 3_download_pdf.py:
download all PDF files that were listed in output.csv, "s3_pdf_path" column.
these PDF files will be stored in the given folder, specified in the python file.
The folder structure will be the same as in S3



# Re-run to update data:
* 2_find_client_pdf_list.py and 3_download_pdf.py will print the line numbers that have been processed.
* The last printed line number should be stored in the python files.

| Python file               | Progress Variable |
| --------------------------| ----------------- |
| 2_find_client_pdf_list.py | finished_line_num |
| 3_download_pdf.py         | last_finished_line_num |

By specifying the Progress variables, the program will skip the lines number that is smaller than the value of the variable. line numbers are 1-based, different from common way that 0-based numbers.

# Assumed roles
In order to pull data from AWS, it is required to use aws-okta command to spedify roles.

*  1_export_subjects.py directly access prism database, no need to specify roles.

* 2_find_client_pdf_list.py should be updated with token for prod environment
  * command to generate this prod token:
  aws-okta exec prod-e2e-ops-support  -- hli-systems-cli auth authenticate --env prod --iam-role-name prod-e2e-ops-support   --mount-point ops_aws

* 3_download_pdf.py
  * the command to run:
  aws-okta exec prod-e2e-ops-support -- python3 3_download_pdf.py