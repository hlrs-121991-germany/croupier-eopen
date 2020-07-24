#!/bin/bash

ftp_username=$1
ftp_password=$2
dest_location=$3

echo $ftp_username
echo $ftp_password
scp test-eopen:~/download_folder/ $dest_location                                           
