import os
import sys
import optparse
import time
import json
from google.cloud import monitoring_v3


def process_google_service_account(service_account, outDir):
    # Set the value GOOGLE_APPLICATION_CREDENTIALS variable
    os.environ.setdefault('GOOGLE_APPLICATION_CREDENTIALS', service_account)
    print("Processing Google Account with credentials: ", os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

    f = open (service_account, "r")
    data = json.loads(f.read())
    f.close()

    project_id = data['project_id']


    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - 604800), "nanos": nanos},
        }
    )
    aggregation = monitoring_v3.Aggregation(
        {
            "alignment_period": {"seconds": 604800},  # 1 week
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MAX,
        }
    )

    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": 'metric.type = "redis.googleapis.com/commands/calls"',
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            "aggregation": aggregation,
        }
    )
    for result in results:
        print(result)


def main():
    if not sys.version_info >= (3, 6):
        print("Please upgrade python to a version at least 3.6")
        exit(1)

    parser = optparse.OptionParser()
    parser.add_option(
        "-d", 
        "--out-dir", 
        dest="outDir", 
        default=".",
        help="The directory to output the results. If the directory does not exist the script will try to create it.", 
        metavar="PATH"
    )

    (options, _) = parser.parse_args()

    if not os.path.isdir(options.outDir):
        os.makedirs(options.outDir)

    # Scan for .json files in order to find the service account files
    path_to_json = '.'
    service_accounts = [os.path.abspath(os.path.join(path_to_json, pos_json)) for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    print(service_accounts)

    # For each service account found try to fetch the metrics using the 
    # google cloud monitoring api metrics
    for service_account in service_accounts:
        process_google_service_account(service_account, options.outDir)


if __name__ == "__main__":
    main()
