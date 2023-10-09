import os
import warnings
from typing import Literal

import numpy as np
import pandas as pd
from bson import ObjectId
from database import CustomDbConnector, Database
from timed import timed


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

    def __init__(self, db: "Database", activity_line_limit: int = 2500):
        self.activity_line_limit = activity_line_limit
        self.db = db

    @property
    def imported(self) -> bool:
        in_db = self.db.list_collection_names()
        return len(in_db) != 0

    @staticmethod
    def _add_mongo_object_id(df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper method to add a MongoDB Object ID column to a dataframe
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
        return users_df

    def _get_transportation_mode(
        self, users_df: pd.DataFrame, activities_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Fill transportation modes for activities, using only exact matches on start_datetime and end_datetime for each user.
        If there are multiple matches, use the first match.
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
                        # Read the track points for an activity
                        df = pd.read_csv(
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
                        df["activity_id"] = activity_id

                        track_point_dfs.append(df)
                        activity_tuples.append(
                            (
                                activity_id,
                                user_id,
                                df["datetime"].iloc[0],
                                df["datetime"].iloc[-1],
                            )
                        )

        # Combine the individual DFs for track points per activity into one large DF
        track_points_df = pd.concat(track_point_dfs, axis=0)
        # Combine all activity data into a DF
        activities_df = pd.DataFrame(
            activity_tuples,
            columns=["_id", "user_id", "start_datetime", "end_datetime"],
        )
        activities_df = self._get_transportation_mode(users_df, activities_df)
        return activities_df, track_points_df

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

        # Add mongo object IDs to all track points
        track_points_df = self._add_mongo_object_id(track_points_df)

        # Create a DataFrame of all activities for a user
        user_activities = pd.DataFrame(
            activities_df.groupby(["user_id"])["_id"]
            .apply(list)
            .reset_index()
            .rename({"_id": "activities", "user_id": "_id"}, axis=1)
        )
        # Add a backreference to the user DF to easily list all activities for a user
        users_df = users_df.merge(user_activities, on="_id", how="left")
        # Fill NA with empty array for users without activities
        users_df["activities"] = users_df["activities"].fillna("").apply(list)

        # Create a DataFrame of all track points for an activity
        activity_track_points = pd.DataFrame(
            track_points_df.groupby(["activity_id"])["_id"]
            .apply(list)
            .reset_index()
            .rename({"_id": "track_points", "activity_id": "_id"}, axis=1)
        )
        # Add a backrefernece to the activities DF to easily list all track points for an activity
        activities_df = activities_df.merge(activity_track_points, on="_id", how="left")
        # Fill NA with empty array for activities without track points
        activities_df["track_points"] = (
            activities_df["track_points"].fillna("").apply(list)
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

        self.imported = True

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
        self.imported = False


def main():
    db_conn = CustomDbConnector()
    importer = Importer(db_conn.db)
    importer.drop_collections()
    importer.import_with_backreferences()


if __name__ == "__main__":
    main()
