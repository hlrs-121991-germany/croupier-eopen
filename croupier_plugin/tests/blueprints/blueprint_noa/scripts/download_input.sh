#!/bin/bash

ftp_username=$1
ftp_password=$2
dest_location=$3
src_location=$4

echo $ftp_username &> ./ftp_download_input_username.txt
echo $ftp_password &> ./ftp_download_input_password.txt
echo $dest_location &> ./ftp_download_input_dest_location.txt
echo $src_location &> ./ftp_download_input_src_location.txt
# Replace SCP with FTP client libraries
scp -r test-eopen:~/EOPEN-Test/ $dest_location                                           
