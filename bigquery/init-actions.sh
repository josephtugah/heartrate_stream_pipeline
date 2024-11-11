#!/bin/bash
# This script installs additional Python libraries on Dataproc nodes

# Update package list and install pip if necessary
apt-get update
apt-get install -y python3-pip

# Install required Python libraries
pip3 install google-cloud-bigquery google-cloud-storage faker pandas pytest



