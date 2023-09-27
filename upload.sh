#!/bin/bash
scp  -i "jay-pdx-prod.pem"  5_find_missing_orders_one_file.py  ec2-user@10.1.76.56:/home/ec2-user/download_report_data/
