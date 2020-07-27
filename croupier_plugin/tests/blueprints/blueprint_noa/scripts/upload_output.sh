#!/bin/bash

ftp_username=$1
ftp_password=$2
file_location=$3

echo $ftp_username
echo $ftp_password


scp $file_location test-eopen:~/EOPEN-Test/
