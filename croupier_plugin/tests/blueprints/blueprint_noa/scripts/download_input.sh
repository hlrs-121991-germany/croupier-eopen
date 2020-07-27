#!/bin/bash

ftp_username=$1
ftp_password=$2
dest_location=$3

echo $ftp_username
echo $ftp_password
scp -r test-eopen:~/EOPEN-Test/ $dest_location                                           
