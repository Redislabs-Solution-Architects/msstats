#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 service_account"
    exit 1
fi

projects=$(gcloud projects list | awk 'NR>1 {print $1}')

echo "$projects" | while read -r project_id; do
    project_number=$(gcloud projects describe ${project_id} --format="value(projectNumber)")
    gcloud projects add-iam-policy-binding ${project_number} --member=serviceAccount:$1 --role='roles/monitoring.viewer' --condition=None
done