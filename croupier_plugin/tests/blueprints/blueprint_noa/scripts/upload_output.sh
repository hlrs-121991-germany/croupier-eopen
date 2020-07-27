#!/bin/bash

ftp_username=$1
ftp_password=$2
upload_location=$4
download_location=$3

echo $ftp_username &> ./ftp_upload_output_username.txt
echo $ftp_password &> ./ftp_upload_output_password.txt
echo $upload_location &> ./ftp_upload_output_upload_location.txt
echo $download_location &> ./ftp_upload_output_download_location.txt
# Replace SCP with FTP client libraries
scp $upload_location test-eopen:~/EOPEN-Test/
