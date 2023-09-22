from database import Database
import os
from environs import Env

class Geolife:
    def __init__(self, database: Database):
        self.database = database
    
    def migrate(self):
        """
        Run all migrations in the migrations directory in lexicographical order.
        Assumes idempotency of migrations, i.e. that running a migration twice
        will not cause any errors.
        """

        # Get all migration files in the migrations directory
        package_dir = os.path.dirname(os.path.abspath(__file__))
        migration_dir = os.path.join(package_dir, "migrations")
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
        raise NotImplementedError()

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

    Geolife(database).migrate()

if __name__ == "__main__":
    main()

