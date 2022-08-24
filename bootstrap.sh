#!/bin/bash

# AWS EMR bootstrap script
#installs git and python package configparser

if grep isMaster /mnt/var/lib/info/instance.json | grep false;
then
    echo "This is not master node, do nothing,exiting"
    exit 0
fi
# continue with code logic for master node below

sudo yum install git -y
sudo pip install configparser

exit 0

