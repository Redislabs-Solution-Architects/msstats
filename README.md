# MSSTATS

MSStats is a tool for extracting MemoryStore database metrics. The script is able to process all the Redis databases, both single instance and replicated (Basic or Standard) ones that belong to a specific service account. Multiple service accounts can be used at once. 

The script will purely use google cloud monitoring api for getting the metrics. It will never connect to the Redis databases and it will NOT send any commands to the databases.

This script by no means will affect the performance and the data stored in the Redis databases it is scanning.


## Installation
### Prerequisites - Software to Install
* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)


The script will run on any system with Python 3.9 or greater installed. If you receive dependency errors, try a more recent version of Python. Python is an ever changing environment and things change that are out of our control.

### Running the script from source

Download the repository

```
git clone https://github.com/Redislabs-Solution-Architects/msstats && cd msstats
```

Prepare and activate the virtual environment.

Ensure you have the right version of python in your path. On a Mac it might be `python3` as Mac as v. 2.7 installed by default.

```
python -m venv .env && source .env/bin/activate
```

Install necessary libraries and dependencies

```
pip install -r requirements.txt
```

Copy your service account .json files in the root directory of the project:

```
cp path/to/service_account.json .
```

Execute 

```
python msstats.py
```

This generates a file named <your project>.xlsx. You need to get that file and send it to Redis.



## Running MSStats Tool in Batch Mode
### Prerequisites - Software to Install
* [GCloud command line tools](https://cloud.google.com/sdk/docs/install)

## Installation
Follow the installation process as described in the previous section and apply the following before executing the script.


Grant monitoring.viewer role to the service account in all associated Google Cloud projects

```
./grant_sa_monitoring_viewer.sh <service_account>
```
For example,
```
./grant_sa_monitoring_viewer.sh gmflau-sa@gcp-dev-day-nyc.iam.gserviceaccount.com
```

Execute

```
./get_msstats.sh <service_account>
```
For example,
```
./get_msstats.sh gmflau-sa@gcp-dev-day-nyc.iam.gserviceaccount.com
```

Remove monitoring.viewer role from the service account in all associated Google Cloud projects 

```
./remove_sa_monitoring_viewer.sh <service_account>
```
For example,
```
./remove_sa_monitoring_viewer.sh gmflau-sa@gcp-dev-day-nyc.iam.gserviceaccount.com
```

When finished do not forget to deactivate the virtual environment

```
deactivate
```
