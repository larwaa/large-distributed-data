import pandas as pd
from database import Database
from timed import timed
import math
from typing import Literal


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
            {"$project": {"year": { "$year": "$start_datetime" }}},

            {"$group": {"_id": "$year", "activityCount": { "$sum": 1 }}},

            {"$project": {"year": "$_id",  "activityCount": 1, "_id": 0}},
                
            {"$sort": {"activityCount": -1}},

            {"$limit": 1}

            ]
        )
        return pd.DataFrame(list(res))


    def task6b(self):
        """
        6. 
            b) Is this also the year with most recorded hours?
        """
        res = self.db.activities.aggregate([
          
        {"$project": {
        "year": { "$year": "$start_datetime" },
        "duration": { "$divide": [{ "$subtract": ["$end_datetime", "$start_datetime"] }, 3600000] }
        }},
        {"$group": {
            "_id": "$year",
            "totalHours": { "$sum": "$duration" }
        }},
        {"$sort": {"totalHours": -1}},
        {"$project": {"year": "$_id",  "totalHours": 1, "_id": 0}},
        {"$limit": 1}

        ])

        return pd.DataFrame(list(res))

























#     [
#     {
#         "$project": {
#             "year": { "$year": "$start_datetime" },
#             "duration": { "$divide": [{ "$subtract": ["$end_datetime", "$start_datetime"] }, 3600000] }
#         }
#     },
#     {
#         "$group": {
#             "_id": "$year",
#             "activityCount": { "$sum": 1 },
#             "totalHours": { "$sum": "$duration" }
#         }
#     },
#     {
#         "$sort": {"activityCount": -1}  # Sort by activityCount in descending order
#     },
#     {
#         "$limit": 1  # Limit to the top result (year with the most activities)
#     }
# ]

        

    @timed
    def task10(self, _type: Literal["box", "circle"] = "box"):
        """
        Find the users who have recorded track points inside the Forbidden City of Beijing.
        As the Forbidden City is quite rectangular, this method returns the IDs of all users
        who have recorded an activity bounded by the box with the following coordinates:
        - Bottom left:
            longitude: 116.392626
            latitude: 39.913349
        - Upper right:
            longitude: 116.401370
            latitude: 39.922705
        Which approximately correspond to the boundaries of the Forbidden City.

        This is an approximation, as $box uses planar geometry. However, since the bounding rectangle
        is small compared to the surface of the earth, we consider the distortion due to the curvature of the
        earth to be insignificant.

        The method relies on a spatial index on the 'location' field on track points, which
        is set as we import the data set. Interally, we use the
        [$geoWithin](https://www.mongodb.com/docs/manual/reference/operator/query/geoWithin/) operator

        Alternatively, by passing `_type="circle"`, we return the users who have recorded a
        track point inside a circle of radius `sqrt(720 000 / pi) ≈ 479 m`. However, users who
        have been to the far edges of the Forbidden City might get left out.

        When using `_type="circle"`, we use the
        [$geoNear aggregation operator](https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/#mongodb-pipeline-pipe.-geoNear)
        which requires a geospatial index on the location field, which is set during import.

        """
        from bson.son import SON

        if _type == "box":
            upper_right_coordinates = [116.401370, 39.922705]
            bottom_left_coordinates = [116.392626, 39.913349]

            box = self.db.track_points.find(
                {
                    "location": {
                        # Find all track points that are inside the bounding rectangle of the Forbidden City
                        "$geoWithin": SON(
                            [
                                (
                                    # https://www.mongodb.com/docs/manual/reference/operator/query/box/
                                    "$box",
                                    [bottom_left_coordinates, upper_right_coordinates],
                                )
                            ]
                        )
                    }
                }
            ).distinct("user_id")
            return pd.DataFrame(list(box))
        else:
            target_latitude = 39.916  # center latitude coordinate of the forbidden city
            target_longitude = (
                116.397  # center longitude coordinate of the forbidden city
            )
            target_size_m2 = 720_000  # size of the Forbidden City in square meters
            max_distance_m = (
                math.sqrt(target_size_m2 / math.pi) + 300
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
