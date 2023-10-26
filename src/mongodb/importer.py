import os
import warnings
from typing import Literal

import numpy as np
import pandas as pd
from bson import ObjectId
from database import CustomDbConnector, Database
from timed import timed
import pymongo


class Importer:
    """
    Class to manage imports to MongoDB.
    """

    # Collections for this project
    collections = ["users", "activities", "track_points"]
    # Number of header rows in activity files
    HEADER_SIZE = 6

    # Path to the dataset, relative to this file
    dataset_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "dataset"
    )
    # Path to the data directory in dataset, relative to this file
    data_path = os.path.join(dataset_path, "data")

    _raw_activities_df: pd.DataFrame
    _raw_track_points_df: pd.DataFrame
    _raw_users_df: pd.DataFrame

    def __init__(self, db: "Database", activity_line_limit: int = 2500):
        self.activity_line_limit = activity_line_limit
        self.db = db

    @property
    def imported(self) -> bool:
        """
        Check if we already have some collections in our database

        Returns:
            bool
                True if we already have imported data, i.e. created collections in our database,
                False otherwise.
        """
        in_db = self.db.list_collection_names()
        return len(in_db) != 0

    @staticmethod
    def _add_mongo_object_id(df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper method to add a MongoDB Object ID column to a dataframe

        Params:
            df: DataFrame
                DataFrame to which to add an `_id` column, which is a Mongo Object ID
        Returns:
            DataFrame
                A copy of `df` with an additional `_id` column
        """
        df_copy = df.copy()
        df_copy["_id"] = df_copy.apply(lambda _: ObjectId(), axis=1)
        return df_copy

    def _import(
        self,
        df: pd.DataFrame,
        collection: Literal["users", "activities", "track_points"],
    ):
        """
        Helper method to import data from a dataframe to a MongoDB collection
        """
        return self.db[collection].insert_many(df.to_dict(orient="records"))

    @timed
    def import_user_df(self) -> pd.DataFrame:
        """
        Import users from user directories and `labeled_ids.txt` into a dataframe of:
            _id: int, int of directory name
            has_labels: bool, true if user has any transporation mode labels, false otherwise
        """
        # List all user directories in the data path, excluding hidden folders
        # as the directory names correspond to user IDs
        user_directories = sorted(
            [int(f) for f in os.listdir(self.data_path) if not f.startswith(".")]
        )
        # Create a df with all user ids
        users_df = pd.DataFrame(user_directories, columns=["_id"]).sort_values("_id")

        # Create a DataFrame with all users who have labeled data
        labeled_users = pd.read_csv(
            os.path.join(self.dataset_path, "labeled_ids.txt"),
            names=["_id"],
            dtype={"_id": np.int64},
        ).sort_values("_id")

        # Add a `has_label` field for all users
        users_df["has_labels"] = users_df["_id"].isin(labeled_users["_id"])
        self._raw_users_df = users_df
        return users_df

    def _add_transportation_mode_to_activities(
        self, users_df: pd.DataFrame, activities_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Fill transportation modes for activities, using only exact matches on start_datetime and end_datetime for each user.
        If there are multiple matches, use the first match.

        Params:
            users_df: pd.DataFrame
                The DataFrame with user data. Must have a `_id` column which is the ID of the user and a `has_labels`
                column, which is a boolean column that is `True` if the user has labelled transportation mode.
            activities_df: pd.DataFrame
                The DataFrame with activity data
        Returns:
            DataFrame
                A copy of `activities_df` with an additional `transportation_mode` column
        """
        label_dfs = []

        for user_id in users_df[users_df["has_labels"]]["_id"]:
            id_with_leading_zeros = "{:03d}".format(user_id)
            labels_file_path = os.path.join(
                self.data_path, id_with_leading_zeros, "labels.txt"
            )
            labels_df = pd.read_csv(
                labels_file_path,
                skiprows=1,
                names=["start_datetime", "end_datetime", "transportation_mode"],
                parse_dates=["start_datetime", "end_datetime"],
                sep="\t",
            )
            labels_df["user_id"] = user_id
            label_dfs.append(labels_df)

        labels_df = pd.concat(label_dfs)

        activities_df_copy = activities_df.merge(
            labels_df.drop_duplicates(["start_datetime", "end_datetime", "user_id"]),
            on=["start_datetime", "end_datetime", "user_id"],
            how="left",
        )
        activities_df_copy["transportation_mode"] = activities_df_copy[
            "transportation_mode"
        ].fillna("")
        return activities_df_copy

    def _add_transportation_mode_to_track_points(
        self, activities_df: pd.DataFrame, track_points_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Adds `transportation_mode` to each track point

        Params:
            activities_df: pd.DataFrame
                The DataFrame with activities data, must have a `transportation_mode` column, which can
                be created by using `_add_transportation_mode_to_activities`
            track_points_df: pd.DataFrame
                The DataFrame with track points. Must have an `activity_id` column

        Returns:
            DataFrame
                A copy of `track_points_df` with an additional `transportation_mode` column
        """
        # Rename and restructure the activities DataFrame to make it easier to merge
        transportation_mode_per_activity = activities_df[
            ["_id", "transportation_mode"]
        ].rename({"_id": "activity_id"}, axis=1)

        # Merge the transportation mode based on activity id
        return track_points_df.merge(
            transportation_mode_per_activity,
            left_on="activity_id",
            right_on="activity_id",
            how="left",
        )

    @timed
    def import_activities_and_track_points_df(
        self, users_df: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        To avoid having to use a combined key for track points and activities, we import these in one bulk and assign
        each activity with a MongoDB object ID which we can use as a reference for track points
        """

        track_point_dfs = []
        activity_tuples = []

        for user_id in users_df["_id"]:
            # User IDs are integers in the DF, but the tracjectories expect
            # the IDs to have leading zeros, so we add them
            id_with_leading_zeros = "{:03d}".format(user_id)

            # Get a list of all activities for the user
            activity_path = os.path.join(
                self.data_path, id_with_leading_zeros, "Trajectory"
            )
            activity_file_names = os.listdir(activity_path)
            for file_name in activity_file_names:
                file_path = os.path.join(activity_path, file_name)
                # if the activity exceeds 2500 track points, we abort early and ignore the activity
                with open(file_path, "r") as f:
                    if (
                        len(f.readlines())
                        <= self.activity_line_limit + self.HEADER_SIZE
                    ):
                        # Create a DataFrame of all track points for this activity
                        curr_tps_df = pd.read_csv(
                            file_path,
                            skiprows=self.HEADER_SIZE,
                            names=[
                                "latitude",
                                "longitude",
                                "na",
                                "altitude",
                                "days_date",
                                "date",
                                "time",
                            ],
                            usecols=[
                                "latitude",
                                "longitude",
                                "altitude",
                                "date",
                                "time",
                            ],
                            dtype={
                                "latitude": np.float64,
                                "longitude": np.float64,
                                "altitude": np.float64,
                            },
                            parse_dates=[["date", "time"]],
                        ).rename({"date_time": "datetime"}, axis=1)

                        # Generate a unique ID for the activity that we can use as a reference for the track points
                        activity_id = ObjectId()
                        curr_tps_df["location"] = curr_tps_df.apply(
                            lambda row: {
                                "type": "Point",
                                "coordinates": [row["longitude"], row["latitude"]],
                            },
                            axis=1,
                        )
                        # Add a reference to the activity id
                        curr_tps_df["activity_id"] = activity_id
                        # Add a reference to the user
                        curr_tps_df["user_id"] = id_with_leading_zeros

                        # Add the DataFrame for the track points for this activity to the list of
                        # all track point DataFrames
                        track_point_dfs.append(curr_tps_df)
                        # Add this activity to the tuple of activity data
                        activity_tuples.append(
                            (
                                activity_id,
                                user_id,
                                # Start datetime for the activity is the datetime of the first track point
                                curr_tps_df["datetime"].iloc[0],
                                # End datetime for the activity is the datetime of the last track point
                                curr_tps_df["datetime"].iloc[-1],
                            )
                        )

        # Combine the individual DFs for track points per activity into one large DF
        track_points_df = pd.concat(track_point_dfs, axis=0)

        # Combine all activity data into a DF
        activities_df = pd.DataFrame(
            activity_tuples,
            columns=["_id", "user_id", "start_datetime", "end_datetime"],
        )

        # Add transportation modes for each activity
        activities_df = self._add_transportation_mode_to_activities(
            users_df, activities_df
        )

        # To make it easier to query the data, we add transportation mode to each track point as well
        track_points_df = self._add_transportation_mode_to_track_points(
            activities_df, track_points_df
        )

        # Add mongo object IDs to all track points
        track_points_df = self._add_mongo_object_id(track_points_df)

        return activities_df, track_points_df

    def _add_back_reference_for_track_points_on_activities(
        self, activities_df: pd.DataFrame, track_points_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Return a copy of `activities_df` with an additional `track_points` column, which is a list of
        track points for each activity.

        Params:
            activities_df: pd.DataFrame
                The DataFrame with activities, must have a column with `_id`, which is the ID of the activity
            track_points_df: pd.DataFrame
                The DataFrame with track points, must have a column with `activity_id`, which is the id of the
                activity the track point belongs to.
        Returns:
            DataFrame
                A copy of `activities_df` with an additional `track_points` column, which is a list of
                track point IDs for each activity.
        """

        # Create a DataFrame of all track points for an activity
        activity_track_points = pd.DataFrame(
            track_points_df.groupby(["activity_id"])["_id"]
            .apply(list)
            .reset_index()
            .rename({"_id": "track_points", "activity_id": "_id"}, axis=1)
        )
        # Add a back reference to the activities DF to easily list all track points for an activity
        activities_with_back_reference = activities_df.merge(
            activity_track_points, on="_id", how="left"
        )
        # Fill NA with empty array for activities without track points
        activities_with_back_reference["track_points"] = (
            activities_with_back_reference["track_points"].fillna("").apply(list)
        )
        return activities_with_back_reference

    def _add_back_reference_for_activities_on_users(
        self, users_df: pd.DataFrame, activities_df: pd.DataFrame
    ):
        """
        Return a copy of users_df with an additional `activities` column, which is a list of
        activities for each user.

        Params:
            users_df: pd.DataFrame
                The DataFrame with user data, must have a `_id` column with the ID of the user,
            activities_df: pd.DataFrame
                The DataFrame with activities data, must have a `user_id` column with the ID of the user
                it belongs to
        Returns:
            pd.DataFrame
                A copy of users_df with `activities`, which is a list of all activity IDs for the user
        """
        # Create a DataFrame of all activities for a user
        user_activities = pd.DataFrame(
            activities_df.groupby(["user_id"])["_id"]
            .apply(list)
            .reset_index()
            .rename({"_id": "activities", "user_id": "_id"}, axis=1)
        )
        # Add a backreference to the user DF to easily list all activities for a user
        users_with_back_reference = users_df.merge(
            user_activities, on="_id", how="left"
        )
        # Fill NA with empty array for users without activities
        users_with_back_reference["activities"] = (
            users_with_back_reference["activities"].fillna("").apply(list)
        )
        return users_with_back_reference

    @timed
    def import_with_backreferences(self):
        """
        Imports users, activities, and track points with backreferences.
        That is, every object has a reference to the related objects they have.

        Users:
            _id: int, same as the directory names in dataset/data/
            has_label: bool, if the user has labeled the transportation mode or not
            activities: list[ObjectId], backreference to all activities for a user. Order of magnitude: 10^3

        Activities:
            _id: ObjectId, generated on import
            start_datetime: datetime, datetime of first track point in the activity
            end_datetime: datetime, datetime of the last track point in the activity
            transportation_mode: str, mode of transportation, "" if not set
            track_points: list[ObjectId], backreference to all track points for an activity. Order of magnitude: 10^3
            user_id: ObjectId, reference to the user the activity belongs to

        Track Points:
            _id: ObjectId, generated on import
            datetime: datetime, timestamp for the track point
            latitude: float, latitude in degrees
            longitude: float, longitude in degrees
            activity_id: ObjectId, reference to the activity the track point belongs to
        """
        if self.imported:
            warnings.warn("Already imported, run `wipe_collections` and try again")
            return

        users_df = self.import_user_df()
        activities_df, track_points_df = self.import_activities_and_track_points_df(
            users_df
        )

        # Add back references to make querying easier
        # We add the list of all activities for a user on the user document
        users_df = self._add_back_reference_for_activities_on_users(
            users_df, activities_df
        )
        # And the list of all track points for an activity on the activity document
        activities_df = self._add_back_reference_for_track_points_on_activities(
            activities_df, track_points_df
        )

        # Create collections for users, activities, and track points
        self.create_collections()

        # Import data into collections
        print("Importing users")
        self._import(users_df, "users")
        print("Importing activities")
        self._import(activities_df, "activities")
        print("Importing track points")
        self._import(track_points_df, "track_points")

        self.add_indices()

    def add_indices(self):
        """
        We can signficicantly improve the performance of certain queries
        by indexing commonly used fields for the data
        """
        print("Adding indices")
        print("add spatial index for location on track_points")
        # Create GEOSPHERE index for track points
        self.db.track_points.create_index([("location", pymongo.GEOSPHERE)])

        # Create index for track points on activity id
        print("Add index for activity_id on track_points")
        self.db.track_points.create_index("activity_id")
        # Create index for track points datetime
        print("Add index for datetime on track_points")
        self.db.track_points.create_index("datetime")
        print("Finished adding indices")

    @timed
    def create_collections(self):
        """
        Create the collections for the project
        """
        for collection in self.collections:
            self.db.create_collection(collection)

    def drop_collections(self):
        """
        Drop the collections for the project
        """
        for collection in self.collections:
            self.db.drop_collection(collection)

    @timed
    def user_sample(self, limit: int = 10):
        return pd.DataFrame(list(self.db.users.find().limit(limit)))

    @timed
    def activity_sample(self, limit: int = 10):
        return pd.DataFrame(list(self.db.activities.find().limit(limit)))

    @timed
    def track_point_sample(self, limit: int = 10):
        return pd.DataFrame(list(self.db.track_points.find().limit(limit)))


def main():
    db_conn = CustomDbConnector()
    importer = Importer(db_conn.db)
    importer.drop_collections()
    importer.import_with_backreferences()


if __name__ == "__main__":
    main()
