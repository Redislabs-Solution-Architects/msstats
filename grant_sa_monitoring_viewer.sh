#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 service_account"
    exit 1
fi

service_account=$1

# Check if the service account exists
if ! gcloud iam service-accounts list --format="value(email)" | grep -q "^$service_account$"; then
    echo "Error: Service account $service_account does not exist."
    exit 1
fi
# Fetch the list of projects
projects=$(gcloud projects list --format="value(projectId)")

# Check if projects were retrieved
if [[ -z "$projects" ]]; then
    echo "Error: No projects found or failed to retrieve projects."
    exit 1
fi

# Iterate through each project and add IAM policy binding
while IFS= read -r project_id; do
    if [[ -n "$project_id" ]]; then
        project_number=$(gcloud projects describe "$project_id" --format="value(projectNumber)")
        
        if [[ -n "$project_number" ]]; then
            echo "Adding IAM policy binding to project ID: $project_id (Project Number: $project_number)"
            
            gcloud projects add-iam-policy-binding "$project_id" \
                --member="serviceAccount:$service_account" \
                --role="roles/monitoring.viewer" \
                --condition=None
        else
            echo "Error: Unable to retrieve project number for project ID $project_id"
        fi
    else
        echo "Error: Empty project ID"
    fi
done <<< "$projects"