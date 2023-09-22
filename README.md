# Very Large, Distributed Data Volumes

This repository contains the project files for TDT4225 Very Large, Distributed Data Volumes for group 7.

## Setup

This project runs a MySQL server through Docker Compose, and Python locally.

### Prerequisites

- Python 3.11
- Docker

### Database

To start the database, run `docker compose up`. Afterwards, it is available at `localhost:3306`. Note that we purposely avoid mounting the MySQL data to a local volume as we want to easily start with a clean slate, meaning that `docker compose down` will wipe _any_ data in the database.

### Python

1. Install dependencies

```zsh
pip install -r requirements.txt
```

2. Run the code

```zsh
python src/main.py
```

## Data

This project expects a `dataset` directory with:

- `data`: a directory of directories for each user, which in turn contain all activities as `.plt`-files and a `labels.txt` if the user has labeled their activities with the mode of transportation
- `labeled_ids.txt`: A file of user IDs for the users that have labelled their mode of transportation.

Due to licensing, the dataset cannot be published.
