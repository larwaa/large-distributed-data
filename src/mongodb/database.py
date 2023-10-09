from datetime import datetime
from typing import Literal, TypedDict, overload

import pymongo.database as pydb
from bson import ObjectId
from DbConnector import DbConnector
from pymongo.collection import Collection


class User(TypedDict):
    _id: int
    has_labels: bool
    activities: list["ObjectId"]


class Activity(TypedDict):
    _id: "ObjectId"
    start_datetime: "datetime"
    end_datetime: "datetime"
    user_id: int
    transportation_mode: str
    track_points: list["ObjectId"]


class TrackPoint(TypedDict):
    _id: "ObjectId"
    longitude: float
    latitude: float
    datetime: "datetime"
    activity_id: "ObjectId"


class Database(pydb.Database):
    """
    Wrapper around pymongo.database to get more accurate type hints for our domain.
    No behavioral difference, only typing.
    """

    @property
    def users(self) -> Collection[User]:
        return self.db.users

    @property
    def activities(self) -> Collection[Activity]:
        return self.db.activities

    @property
    def track_points(self) -> Collection[TrackPoint]:
        return self.db.track_points

    @overload
    def __getitem__(self, key: Literal["users"]) -> Collection[User]:
        ...

    @overload
    def __getitem__(self, key: Literal["activities"]) -> Collection[Activity]:
        ...

    @overload
    def __getitem__(self, key: Literal["track_points"]) -> Collection[TrackPoint]:
        ...

    @overload
    def __getitem__(self, key: str) -> Collection:
        ...

    def __getitem__(
        self, key: Literal["users", "track_points", "activities"] | str
    ) -> Collection[User] | Collection[Activity] | Collection[TrackPoint] | Collection:
        return super().__getitem__(key)


class CustomDbConnector(DbConnector):
    """
    Simple wrapper around DbConnector to get more accurate type hints when working
    with pymongo, and adjustments to connection configuration to support Docker out of the box

    The only behavioral difference is the constructor, otherwise, it's just type hints which do not affect runtime.
    """

    db: Database

    def __init__(
        self,
        host: str = "localhost",
        database: str = "mongo",
        user: str = "root",
        password: str = "password",
        port: int = 27017,
        connection_opts: str = "?authSource=admin",
    ):
        super().__init__(
            DATABASE=f"{database}{connection_opts}",
            USER=user,
            PASSWORD=password,
            HOST=f"{host}:{port}",
        )
