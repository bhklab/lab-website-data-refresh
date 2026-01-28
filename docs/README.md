# Data Refresh Script for Lab Website

Script built to sync new/current records from google sheets for presentations, publications, and preprints into mongodb that will then surface on the lab website. This script is deployed to run Monday-Friday at 5:00 A.M (EST) on the lab server as a cron job.

## Requirements

- `pixi`

## Quickstart

Navigate to the root directory of the repo and run the following command:

```bash
pixi install
```

- It is key to note that a service account is needed along with its credentials in a json file to run this script. The sheet you wish to read from on google also needs to be shared with the service account so it has proper access. Other envirnoment variables need to be setup as listed in 'scripts/conversion.py' (ie. mongodb connection string).

- Place the .env file in the root of the project environment along with the service account json file. Once this is done, enter the following to run the script:

```bash
pixi run start
```

## Outputs

As the script runs, you will be shown the number of records retrieved from the desired sheet and of the different actions being taken on each collection.
ie. { 'upserted': 21, 'modified': 15, 'matched': 15 }
