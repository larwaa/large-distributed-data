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

task = Task()
print(task.task1())