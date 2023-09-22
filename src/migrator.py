from database import Database
import os
from environs import Env
from timed import timed
from typing import Iterable

class Migrator:
    def __init__(self, database: Database):
        self.database = database
        self.package_dir = os.path.dirname(os.path.abspath(__file__))
        self.migrated = False

    @timed
    def wipe(self):
        """
        Wipe the database of all data and tables.
        """
        print("Wiping TrackPoint")
        self.database.cursor.execute("DROP TABLE IF EXISTS TrackPoint")
        print("Wiping Activity")
        self.database.cursor.execute("DROP TABLE IF EXISTS Activity")
        print("Wiping User")
        self.database.cursor.execute("DROP TABLE IF EXISTS User")
        self.database.connection.commit()
        self.migrated = False
    
    @timed
    def migrate(self) -> None:
        """
        Run all migrations in the migrations directory in lexicographical order.
        Assumes idempotency of migrations, i.e. that running a migration twice
        will not cause any errors.
        """

        # Get all migration files in the migrations directory
        migration_dir = os.path.join(self.package_dir, "migrations")
        migration_files = os.listdir(migration_dir)

        print("Found migration files: ", migration_files)

        # Start a transction
        self.database.connection.start_transaction()
        try:
            # For each file in the directory, in lexicographical order, run the migration
            for migration_file in sorted(migration_files):
                with open(os.path.join(migration_dir, migration_file), "r") as f:
                    print("Running migration:".ljust(20), migration_file.ljust(20), end="")
                    self.database.cursor.execute(f.read())
                    print("✅")
            self.database.connection.commit()
            self.migrated = True
        except Exception as e: 
            print("❌")
            print("Something went wrong:", e)
            self.database.connection.rollback()

    @timed
    def seed(self):
        """
        Part 1:

        Seed the database with data from the dataset.
        """
        assert self.migrated, "You must run the migrations before seeding the database. Run `.migrate()` first."

        self.seed_users()
        self.seed_activities()
        self.seed_track_points()
    
    @timed
    def seed_users(self):
        """
        Seed the users table with the user IDs from the dataset.
        This seed is idempotent, i.e. it can be run multiple times without
        causing any errors.
        """
        dataset_dir = os.path.join(self.package_dir, "dataset")
        data_dir = os.path.join(dataset_dir, "data")
        labeled_ids_filepath = os.path.join(dataset_dir, "labeled_ids.txt")

        # User IDs can be obtained from the directory names in the data directory.
        # Filter on numeric directory names to avoid hidden files and directories.
        user_ids = self._get_user_ids()

        labeled_ids: list[str] = []
        with open(labeled_ids_filepath, "r") as f:
            labeled_ids = f.read().splitlines()

        data: list[tuple[str, bool]] = []
        for user_id in user_ids:
            has_labels = user_id in labeled_ids
            data.append((user_id, has_labels))
        
        query = """
            INSERT INTO User(id, has_labels) VALUES (%s, %s) ON DUPLICATE KEY UPDATE has_labels=VALUES(has_labels)
        """

        self.database.cursor.executemany(query, data)
        self.database.connection.commit()

        self.database.cursor.execute("SELECT * FROM User")
        rows = self.database.cursor.fetchall()
        print(f"Seeded {len(rows)} Users")

    def _get_user_ids(self) -> Iterable[str]:
        dataset_dir = os.path.join(self.package_dir, "dataset")
        data_dir = os.path.join(dataset_dir, "data")

        # User IDs can be obtained from the directory names in the data directory.
        # Filter on numeric directory names to avoid hidden files and directories.
        return filter(lambda dir_name: dir_name.isnumeric(), os.listdir(data_dir))

    @timed
    def seed_activities(self):
        """
        Seed activities to the database from the dataset.
        While the exercise text suggests using `os.walk`, we opt for a different approach
        to have more explicit control over the order of the files.

        We take a two stage approach to seeding the activities table:
        1. Create the activity records for each user, without transportation modes.
        2. Update the activity records with the transportation modes with the power of SQL.
        """
        query = """
            INSERT IGNORE INTO Activity(id, user_id, start_datetime, end_datetime) VALUES (%s, %s, %s, %s)
        """

        data = self._make_activity_data()

        self.database.cursor.executemany(query, data)
        self.database.connection.commit()

        self._update_activity_transportation_modes()

        self.database.cursor.execute("SELECT * FROM Activity")
        rows = self.database.cursor.fetchall()
        print(f"Seeded {len(rows)} Activities")

    def _make_activity_data(self) -> list[tuple[str, str, str, str]]:
        """
        Create data for the activities table.
        Crucially, this is NOT idempotent, i.e. running this function multiple times
        will result in duplicate data.
        """
        data: list[tuple[str, str, str, str]] = []

        user_ids = self._get_user_ids()

        for user_id in user_ids:
            print("Generating seed data for user:", user_id, end="\t")
            activity_files = self._get_activity_files_for_user(user_id)

            # For the remaining files, create an activity record
            for activity_file in activity_files:
                with open(activity_file, "r") as f:
                    # Skip the first 6 lines, as they are headers
                    track_points = f.readlines()[6:]
                    
                    # Get the start and end datetime
                    start_date = track_points[0].split(",")[5].strip()
                    start_time = track_points[0].split(",")[6].strip()
                    end_date = track_points[-1].split(",")[5].strip()
                    end_time = track_points[-1].split(",")[6].strip()

                    start_datetime = f"{start_date} {start_time}"
                    end_datetime = f"{end_date} {end_time}"
                    
                    # To ensure idempotency and consistency, we manually create a unique ID for each
                    # activity.
                    activity_id = self._get_activity_id(activity_file, user_id)
                    
                    data.append((activity_id, user_id, start_datetime, end_datetime))
            print("✅")
        return data
    
    def _update_activity_transportation_modes(self):
        labeled_ids: list[str] = []
        with open(os.path.join(self.package_dir, "dataset", "labeled_ids.txt"), "r") as f:
            labeled_ids = f.read().splitlines()

        for id in labeled_ids:
            labels = self._get_labels_for_user_from_dataset(id)
            query = """
                UPDATE Activity SET transportation_mode = %s WHERE user_id = %s AND start_datetime >= %s AND end_datetime <= %s
            """
            self.database.cursor.executemany(query, labels)
            self.database.connection.commit()

    def _get_labels_for_user_from_dataset(self, id: str) -> list[tuple[str, str, str, str]]:
        labels_filepath = os.path.join(self.package_dir, "dataset", "data", id, "labels.txt")
        with open(labels_filepath, "r") as f:
            labels: list[tuple[str, str, str, str]] = []
            for label in f.readlines()[1:]:
                start_datetime, end_datetime, mode = label.split("\t")
                labels.append((mode.strip(), id, start_datetime, end_datetime))
            return labels

    def _get_activity_files_for_user(self, user_id: str, max_track_points: int = 2500) -> list[str]:
        user_activity_dir = os.path.join(self.package_dir, "dataset", "data", user_id, "Trajectory")
        activity_files = os.listdir(user_activity_dir)
        activity_files_abs_path: list[str] = []
        
        # Filter out files that exceed 2500 track points
        for activity_file in activity_files:
            # Filter out files that exceed 2500 track points
            with open(os.path.join(user_activity_dir, activity_file), "r") as f:
                # Skip the first 6 lines, as they are headers
                track_points = f.readlines()[6:]
                # Only record the activity if we have fewer than 2500 track points
                if len(track_points) <= max_track_points:
                    activity_files_abs_path.append(os.path.join(user_activity_dir, activity_file))
        
        return activity_files_abs_path

    
    @timed
    def seed_track_points(self):
        query = """
            INSERT INTO TrackPoint (activity_id, latitude, longitude, altitude, date_days, datetime) VALUES (%s, %s, %s, %s, %s, %s)
        """

        data: list[tuple[str, str, str, str, str, str]] = []

        data_dir = os.path.join(self.package_dir, "dataset", "data")
        user_ids = self._get_user_ids()

        for user_id in user_ids:
            print("Generating seed data for user:", user_id, end="\t")
            activity_files = self._get_activity_files_for_user(user_id)
            for activity_file in activity_files:
                activity_id = self._get_activity_id(activity_file, user_id)
                with open(activity_file, "r") as f:
                    track_points = f.readlines()[6:]
                    for track_point in track_points:
                        latitude, longitude, _, altitude, date_days, date, time = track_point.split(",")
                        datetime = f"{date} {time.strip()}"
                        data.append((activity_id, latitude, longitude, altitude, date_days, datetime))

            print("✅")

        batch_size = 120_000
        print(data[:20])
        for i in range(0, len(data), batch_size):
            print(f"Seeding batch: {i // batch_size} of {len(data) // batch_size}", end="\t")
            self.database.cursor.executemany(query, data[i:i+batch_size])
            print("✅")

        self.database.connection.commit()
        self.database.cursor.execute("SELECT * FROM TrackPoint")
        rows = self.database.cursor.fetchall()
        print(f"Seeded {len(rows)} TrackPoints")

    def _get_activity_id(self, activity_file_name: str, user_id: str) -> str:
        return f"{user_id}-{activity_file_name.split('/')[-1].split('.')[0]}"


    
def main():
    env = Env()
    env.read_env(".env.development.local")
    env.read_env(".env.development")
    env.read_env(".env")

    database = Database(
        port=env.int("DB_PORT"),
        host=env.str("DB_HOST"),
        user=env.str("DB_USER"),
        password=env.str("DB_PASSWORD"),
        database=env.str("DB_DATABASE")
    )

    geolife = Migrator(database)
    geolife.migrate()
    geolife.seed()

if __name__ == "__main__":
    main()

