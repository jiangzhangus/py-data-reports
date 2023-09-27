#!/bin/bash
scp  -i "jay-pdx-prod.pem"   ec2-user@10.1.76.56:/home/ec2-user/download_report_data/run_log.txt ./
scp  -i "jay-pdx-prod.pem"   ec2-user@10.1.76.56:/home/ec2-user/download_report_data/order_list.csv ./
scp  -i "jay-pdx-prod.pem"   ec2-user@10.1.76.56:/home/ec2-user/download_report_data/order_missing_list.csv ./
