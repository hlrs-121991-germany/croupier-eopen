#!/bin/bash

ftp_username=$1
ftp_username=$2
file_location=$3

scp $file_location test-eopen:~/upload_folder/
