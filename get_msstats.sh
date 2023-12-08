#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 service_account"
    exit 1
fi

projects=$(gcloud projects list | awk 'NR>1 {print $1}')

timestamp=$(date +"%Y%m%d_%H%M%S")

echo "$projects" | while read -r project_id; do
    python msstats.py -p $project_id -r ms-report-${timestamp}
done
