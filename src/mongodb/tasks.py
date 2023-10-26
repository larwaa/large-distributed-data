import pandas as pd
from database import Database
from timed import timed
import math
from typing import Literal
from bson.son import SON
import datetime


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

    @timed
    def task6a(self):
        """
        6.
            a) Find the year with the most activities.
        """

        res = self.db.activities.aggregate(
            [
                {"$project": {"year": {"$year": "$start_datetime"}}},
                {"$group": {"_id": "$year", "activityCount": {"$sum": 1}}},
                {"$project": {"year": "$_id", "activityCount": 1, "_id": 0}},
                {"$sort": {"activityCount": -1}},
                {"$limit": 1},
            ]
        )
        return pd.DataFrame(list(res))

    def task6b(self):
        """
        6.
            b) Is this also the year with most recorded hours?
        """
        res = self.db.activities.aggregate(
            [
                {
                    "$project": {
                        "year": {"$year": "$start_datetime"},
                        "duration": {
                            "$divide": [
                                {"$subtract": ["$end_datetime", "$start_datetime"]},
                                3600000,
                            ]
                        },
                    }
                },
                {"$group": {"_id": "$year", "totalHours": {"$sum": "$duration"}}},
                {"$sort": {"totalHours": -1}},
                {"$project": {"year": "$_id", "totalHours": 1, "_id": 0}},
                {"$limit": 1},
            ]
        )

        return pd.DataFrame(list(res))

    @timed
    def task9(self):
        """
        Find all users who have invalid activities, and the number of invalid activities
        per user
        - An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes.

        We're opting out of doing this with aggregation pipelines as joins are a bit
        tricky.
        """

        # Fetch the track points, sorted by activity_id and datetime
        # to ensure that consecutive track points for the same activity are in fact
        # consecutive elements in our result
        print("Fetching track points")
        track_points = self.db.track_points.find(
            {}, {"activity_id": 1, "datetime": 1, "user_id": 1}, allow_disk_use=True
        ).sort(["activity_id", "datetime"])

        # Load the results into a DataFrame
        print("Loading into DataFrame")
        df = pd.DataFrame(list(track_points))

        # Ensure that the `datetime` column is a `datetime`
        print("Convert datetime to datetime")
        df["datetime"] = pd.to_datetime(df["datetime"])

        # Create a new column, `time_diff`, which is the time difference between two consecutive rows
        print("Find time diff")
        df["time_diff"] = df["datetime"].diff()

        # Create a boolean index of columns where the `time_diff` exceeds 5 minutes
        print("Find indices where time diff exceeds 5 minutes")
        time_diff_gt_5_minutes = df["time_diff"] > datetime.timedelta(minutes=5)

        # We're only interested in track points that have a difference greater than 5 minutes
        # if they belong to the same activity.
        # Thus, create a boolean index for rows that have the same activity_id as the previous row
        print("Find indices where activity id is equal to the previous index")
        same_activity_as_previous_row = df["activity_id"].eq(df["activity_id"].shift())

        # Combine the two boolean indices to find the indices where the time difference to the previous
        # track point exceeds 5 minutes, and has the same activity_id as the previous track point
        print(
            "Find indices where activity id is the same as the previous index AND the time difference exceeds 5 minutes"
        )
        same_activity_and_diff_gt_5_min = (
            same_activity_as_previous_row & time_diff_gt_5_minutes
        )

        # This gives us a DataFrame of invalid track points
        print("Find all invalid track points")
        invalid_track_points = df[same_activity_and_diff_gt_5_min]

        # We're only interested in distinct activity_ids
        print("Drop duplicates on activity id")
        invalid_track_points = invalid_track_points.drop_duplicates(["activity_id"])

        # Now that we have a DataFrame of unique activity ids that are invalid, we
        # group by user_id, and count the number of invalid activity_ids per user
        print("Group by user and count the number of activity ids")
        result = (
            invalid_track_points[["activity_id", "user_id"]]
            .groupby(["user_id"])
            .count()
        )

        return result

    @timed
    def task10(self, how: Literal["polygon", "circle"] = "polygon"):
        """
        Find the users who have recorded track points inside the Forbidden City of Beijing.
        As the Forbidden City is quite rectangular, this method returns the IDs of all users
        who have recorded an activity bounded by the polygon with the following coordinates:
        - Bottom left:
            longitude: 116.392626
            latitude: 39.913349
        - Bottom right:
            longitude: 116.392644
            latitude: 39.913440
        - Upper left:
            longitude: 116.392182
            latitude: 39.922432
        - Upper right:
            longitude: 116.401370
            latitude: 39.922705
        Which approximately correspond to the boundaries of the Forbidden City.

        This is an approximation, as $polygon uses planar geometry. However, since the bounding rectangle
        is small compared to the surface of the earth, we consider the distortion due to the curvature of the
        earth to be insignificant.

        The method relies on a spatial index on the 'location' field on track points, which
        is set as we import the data set. Interally, we use the
        [$geoWithin](https://www.mongodb.com/docs/manual/reference/operator/query/geoWithin/) operator

        Alternatively, by passing `how="circle"`, we return the users who have recorded a
        track point inside a circle of radius `sqrt(720 000 / pi) ≈ 479 m`. However, users who
        have been to the far edges of the Forbidden City might get left out.

        When using `how="circle"`, we use the
        [$geoNear aggregation operator](https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/#mongodb-pipeline-pipe.-geoNear)
        which requires a geospatial index on the location field, which is set during import.

        """
        from bson.son import SON

        if how == "polygon":
            upper_right_coordinates = [116.401370, 39.922705]
            upper_left_coordinates = [116.392182, 39.922432]
            bottom_left_coordinates = [116.392626, 39.913349]
            bottom_right_coordinates = [116.392644, 39.913440]

            polygon = self.db.track_points.find(
                {
                    "location": {
                        # Find all track points that are inside the bounding rectangle of the Forbidden City
                        "$geoWithin": SON(
                            [
                                (
                                    # https://www.mongodb.com/docs/manual/reference/operator/query/polygon/
                                    "$polygon",
                                    [
                                        bottom_left_coordinates,
                                        bottom_right_coordinates,
                                        upper_right_coordinates,
                                        upper_left_coordinates,
                                    ],
                                )
                            ]
                        )
                    }
                }
            ).distinct("user_id")
            return pd.DataFrame(list(polygon), columns=["Users in the Forbidden City"])
        else:
            target_latitude = 39.916  # center latitude coordinate of the forbidden city
            target_longitude = (
                116.397  # center longitude coordinate of the forbidden city
            )
            target_size_m2 = 720_000  # size of the Forbidden City in square meters
            max_distance_m = (
                math.sqrt(target_size_m2 / math.pi)
                + 300  # Add a slight buffer as the circle won't catch the edges.
            )  # Bounding circle

            result = self.db.track_points.aggregate(
                [
                    {
                        # https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/#mongodb-pipeline-pipe.-geoNear
                        "$geoNear": SON(
                            [
                                # set the distance to the forbidden city to the field 'distance_to_forbidden_city'
                                ("distanceField", "distance_to_forbidden_city"),
                                # the field which has location data for the track points
                                # must be a Geosphere 2D index
                                ("key", "location"),
                                # max distance to be considered "near"
                                ("maxDistance", max_distance_m),
                                # target coordinate
                                (
                                    "near",
                                    SON(
                                        [
                                            ("type", "Point"),
                                            (
                                                "coordinates",
                                                [target_longitude, target_latitude],
                                            ),
                                        ]
                                    ),
                                ),
                                # consider a sphere instead of 2d geometry
                                ("nearSphere", True),
                            ]
                        )
                    },
                    # Sort all documents on distance to forbidden city, ascending
                    {"$sort": {"distance_to_forbidden_city": -1}},
                    # Group the documents by user_id, and for track_point, distance, location, and activity ID
                    # we can select the first instance, as the documents have been sorted based on distance.
                    {
                        "$group": {
                            "_id": "$user_id",
                            "nearest_distance_to_hidden_city (m)": {
                                "$first": "$distance_to_forbidden_city"
                            },
                            "track_point_id": {"$first": "$_id"},
                            "activity_id": {"$first": "$activity_id"},
                            "location": {"$first": "$location"},
                        }
                    },
                    # Sort on user ID
                    {"$sort": {"_id": 1}},
                    {
                        "$project": SON(
                            [
                                ("user_id", "$_id"),
                                ("_id", 0),
                                ("nearest_distance_to_hidden_city (m)", 1),
                                ("activity_id", 1),
                                ("track_point_id", 1),
                            ]
                        )
                    },
                ]
            )
            return pd.DataFrame(list(result))

    @timed
    def task11(self):
        """
        Find all users who have registered transportation_mode and their most used
        transportation_mode.
        """
        res = self.db.activities.aggregate(
            [
                {
                    # Exclude activities where transportation_mode is ""
                    "$match": {"transportation_mode": {"$ne": ""}}
                },
                {
                    # Group by user_id and transportation_mode, and count occurances
                    "$group": {
                        "_id": {
                            "user_id": "$user_id",
                            "transportation_mode": "$transportation_mode",
                        },
                        "count": {"$count": {}},
                    }
                },
                {
                    # Group by user_id, and select first occurance of transportation_mode with max count
                    "$group": {
                        # Group by user_id
                        "_id": "$_id.user_id",
                        # Select max count of transportation mode
                        "transportation_mode_count": {"$max": "$count"},
                        # Tie break first mode of transportation
                        "transportation_mode": {"$first": "$_id.transportation_mode"},
                    }
                },
                {
                    # Sort on user_id, ascending
                    "$sort": {
                        "_id": 1,
                    }
                },
                {
                    # Exclude the _id column, return user_id, transportation_mode, and transportation_mode_count
                    "$project": {
                        "_id": 0,
                        "user_id": "$_id",
                        "most_used_transportation_mode": "$transportation_mode",
                        "transportation_mode_count": "$transportation_mode_count",
                    }
                },
            ]
        )
        return pd.DataFrame((list(res)))


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
