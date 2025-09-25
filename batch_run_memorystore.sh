#!/bin/bash

projects=$(gcloud projects list | awk 'NR>1 {print $1}')

echo "$projects" | while read -r project_id; do
    python memorystore.py --project $project_id --credentials /path/to/sa.json --out /path/to/$project_id.csv
done
