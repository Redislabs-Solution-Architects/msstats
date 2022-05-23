# MSSTATS

MSStats is a tool for extracting MemoryStore database metrics. The script is able to process all the Redis databases, both single instance, replicated and clustered ones that belong to a specific service account. Multiple service accounts can be defined in the configuration. 

The script will purely use google cloud monitoring api for getting the metrics. It will never connect to the Redis databases and it will NOT send any commands to the databases.

This script by no means will affect the performance and the data stored in the Redis databases it is scanning.


## Installation

The script will run on any system with Python 3.6 or greater installed.

### Running the script from source

Download the repository

```
git clone https://github.com/Redislabs-Solution-Architects/msstats && cd msstats
```

Prepare and activate the virtual environment

```
python3 -m venv .env && source .env/bin/activate
```

Install necessary libraries and dependencies

```
pip install -r requirements.txt
```

Copy the example configuration file and update its contents to match your configuration:

```
cp config.ini.example config.ini && vim config.ini
```

Execute 

```
python msstats.py -c config.ini
```

When finished do not forget to deactivate the virtual environment

```
deactivate
```