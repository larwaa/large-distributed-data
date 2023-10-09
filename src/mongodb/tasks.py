import pandas as pd
from database import Database
from timed import timed


class Task:
    def __init__(self, db: "Database"):
        self.db = db

    @timed
    def task1(self):
        """
        How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database).
        """

        # Count number of users
        users = self.db.users.count_documents({})
        # Count number of activities
        activities = self.db.activities.count_documents({})
        # Count number of track points
        track_points = self.db.track_points.count_documents({})
        return pd.DataFrame(
            {
                "# users": users,
                "# activities": activities,
                "# track points": track_points,
            },
            index=[0],
        )

    @timed
    def task2(self):
        """
        Find the average number of activities per user.
        """

        res = self.db.users.aggregate(
            [
                # Count the number of activities per user
                {"$project": {"activity_count": {"$size": "$activities"}}},
                {
                    "$group": {
                        # Group everything
                        "_id": {},
                        # Find minimum activity count
                        "min_activity_count": {"$min": "$activity_count"},
                        # Find maximum activity count
                        "max_activity_count": {"$max": "$activity_count"},
                        # Find average activity count
                        "avg_activity_count": {"$avg": "$activity_count"},
                    }
                },
            ]
        )
        return pd.DataFrame(list(res), index=[0]).drop("_id", axis=1)

    @timed
    def task3(self):
        """
        Find the top 20 users with the highest number of activities.
        """

        res = self.db.users.aggregate(
            [
                # Count the number of activities for each user
                {"$project": {"activity_count": {"$size": "$activities"}}},
                # Sort by activitiy count, descending
                {"$sort": {"activity_count": -1}},
                # Limit to 20
                {"$limit": 20},
            ]
        )
        return pd.DataFrame(list(res))

    @timed
    def task4(self):
        """
        Find all users who have taken a taxi.
        """

        res = self.db.activities.find(
            # Find users that have taken a taxi
            {"transportation_mode": "taxi"},
            # Only include user_id
            {"user_id": 1},
        ).distinct("user_id")
        return pd.DataFrame(list(res), columns=["Users Who Have Taken a Taxi"])

    @timed
    def task5(self):
        """
        Find all types of transportation modes and count how many activities that are
        tagged with these transportation mode labels. Do not count the rows where
        the mode is null.
        """

        res = self.db.activities.aggregate(
            [
                # Remove entries without transportation mode, i.e. ""
                {"$match": {"transportation_mode": {"$ne": ""}}},
                # Group by transportation mode, using count aggregation
                {
                    "$group": {
                        "_id": "$transportation_mode",
                        "transportation_mode_count": {"$count": {}},
                    }
                },
                # Sort by count per transportation mode, descending
                {"$sort": {"transportation_mode_count": -1}},
            ]
        )
        return pd.DataFrame(list(res))


def main():
    """
    Run through all tasks, saving the answers to disk.
    """

    from database import CustomDbConnector

    db_conn = CustomDbConnector()
    task = Task(db_conn.db)

    task.task1().to_csv("task1.csv")
    task.task2().to_csv("task2.csv")
    task.task3().to_csv("task3.csv")
    task.task4().to_csv("task4.csv")
    task.task5().to_csv("task5.csv")


if __name__ == "__main__":
    main()
