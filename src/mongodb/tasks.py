import pandas as pd
from database import Database
from timed import timed


class Task:
    def __init__(self, db: "Database"):
        self.db = db

    @timed
    def task1(self):
        users = self.db.users.count_documents({})
        activities = self.db.activities.count_documents({})
        track_points = self.db.track_points.count_documents({})
        return pd.DataFrame({"# users": users, "# activities": activities, "# track points": track_points}, index=[0])

    @timed
    def task2(self):
        res = self.db.users.aggregate([
            {
                "$project": {
                    "activity_count": {
                        "$size": "$activities"
                    }
                }
            },
            {
                "$group": {
                    "_id": {},
                    "min_activity_count": {
                        "$min": "$activity_count"
                    } ,
                    "max_activity_count": {
                        "$max": "$activity_count"
                    },
                    "avg_activity_count": {
                        "$avg": "$activity_count"
                    }
                }
            },
        ])
        return pd.DataFrame(list(res), index=[0]).drop("_id", axis=1)
    
    @timed
    def task3(self):
        res = self.db.users.aggregate([
            {
                "$project": {
                    "activity_count": {
                        "$size": "$activities"
                    }
                }
            },
            {
                "$sort": {
                    "activity_count": -1
                }
            },
            {
                "$limit": 20
            }
        ])
        return pd.DataFrame(list(res))


def main():
    from database import CustomDbConnector
    db_conn = CustomDbConnector()
    task = Task(db_conn.db)

    print(task.task1())
    print(task.task2())
    print(task.task3())


if __name__ == "__main__":
    main()