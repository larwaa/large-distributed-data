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

## Citations

[1] Yu Zheng, Lizhu Zhang, Xing Xie, Wei-Ying Ma. [Mining interesting locations and travel sequences from GPS trajectories](http://research.microsoft.com/apps/pubs/?id=79440). In
Proceedings of International conference on World Wild Web (WWW 2009), Madrid Spain. ACM Press: 791-800.

[2] Yu Zheng, Quannan Li, Yukun Chen, Xing Xie, Wei-Ying Ma. [Understanding Mobility Based on GPS Data](http://research.microsoft.com/apps/pubs/?id=77984). In Proceedings of
ACM conference on Ubiquitous Computing (UbiComp 2008), Seoul, Korea. ACM Press: 312-321.

[3] Yu Zheng, Xing Xie, Wei-Ying Ma, [GeoLife: A Collaborative Social Networking Service among User, location and trajectory](http://research.microsoft.com/apps/pubs/?id=131038).
Invited paper, in IEEE Data Engineering Bulletin. 33, 2, 2010, pp. 32-40.
