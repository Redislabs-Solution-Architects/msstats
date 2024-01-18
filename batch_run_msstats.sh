#!/bin/bash

projects=$(gcloud projects list | awk 'NR>1 {print $1}')

echo "$projects" | while read -r project_id; do
    python msstats.py -p $project_id
done
