from database import Database
import os
from environs import Env

class Geolife:
    def __init__(self, database: Database):
        self.database = database
        self.package_dir = os.path.dirname(os.path.abspath(__file__))
    
    def migrate(self) -> None:
        """
        Run all migrations in the migrations directory in lexicographical order.
        Assumes idempotency of migrations, i.e. that running a migration twice
        will not cause any errors.
        """

        # Get all migration files in the migrations directory
        migration_dir = os.path.join(self.package_dir, "migrations")
        migration_files = os.listdir(migration_dir)

        print("Migration files: ", migration_files)

        # Start a transction
        self.database.connection.start_transaction()
        try:
            # For each file in the directory, in lexicographical order, run the migration
            for migration_file in sorted(migration_files):
                with open(os.path.join(migration_dir, migration_file), "r") as f:
                    print("Running migration: " + migration_file + "...")
                    self.database.cursor.execute(f.read())
                    print("Migration done!")
            self.database.connection.commit()
        except: 
            self.database.connection.rollback()

    def seed(self):
        self._seed_users()
    
    def _seed_users(self):
        dataset_dir = os.path.join(self.package_dir, "dataset")
        data_dir = os.path.join(dataset_dir, "data")
        labeled_ids_filepath = os.path.join(dataset_dir, "labeled_ids.txt")

        # User IDs can be obtained from the directory names in the data directory.
        # Filter on numeric directory names to avoid hidden files and directories.
        user_ids = filter(lambda dir_name: dir_name.isnumeric(), os.listdir(data_dir))

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

        print("Inserting users")
        self.database.cursor.executemany(query, data)
        self.database.connection.commit()

        print("Fetching users")
        self.database.cursor.execute("SELECT * FROM User")
        print(self.database.cursor.fetchall())


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

    geolife = Geolife(database)
    geolife.migrate()
    geolife.seed()

if __name__ == "__main__":
    main()

