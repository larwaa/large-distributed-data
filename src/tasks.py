from database import Database
from timed import timed

class Task:
    def __init__(self, db: Database) -> None:
        self.db = db
    
    @timed
    def task1(self):
        """
        How many users, activities and trackpoints are there in the dataset
        (after it is inserted into the database).
        """

        count_query = """
        SELECT
            (SELECT Count(*) AS UsersCount FROM Users) AS '# Users',
            (SELECT Count(*) AS UsersCount FROM Activities) AS '# Activities',
            (SELECT Count(*) AS UsersCount FROM TrackPoints) AS '# TrackPoints';
        """


        return self.db.query(count_query)
    
    @timed
    def task2(self):
        query = """
            SELECT CAST(ROUND(AVG(count), 0) AS SIGNED) AS Avg, MAX(count) AS Max, MIN(count) AS Min
            FROM (
                SELECT COUNT(*) AS count
                FROM TrackPoints as tp
                LEFT JOIN Activities as a
                    ON tp.activity_id = a.id
                GROUP BY a.user_id
            ) as counts;
        """

        return self.db.query(query)
    
    @timed
    def task3(self):
        query = """
            SELECT UserId, ActivityCount
            FROM
                (
                    SELECT Count(*) as ActivityCount, u.id as UserId
                    FROM Activities as a
                    LEFT JOIN Users as u
                        on a.user_id = u.id
                    GROUP BY u.id
                ) as activityCounts
            ORDER BY ActivityCount DESC
            LIMIT 15;
        """
        return self.db.query(query)
    
    @timed
    def task4(self):
        query = """
            SELECT DISTINCT u.id AS UserId
            FROM Users AS u
            LEFT JOIN Activities AS a
                ON u.id = a.user_id
            WHERE a.transportation_mode LIKE 'Bus';
        """

        return self.db.query(query)
    
    @timed
    def task5(self):
        query = """
            SELECT DISTINCT u.id AS UserID, Count(DISTINCT a.transportation_mode) as '# Transportation Modes', GROUP_CONCAT(DISTINCT a.transportation_mode SEPARATOR ', ') AS 'Transportation Modes'
            FROM Activities AS a
            LEFT JOIN Users AS u
                ON a.user_id = u.id
            WHERE a.transportation_mode != ""
            GROUP BY u.id
            ORDER BY Count(DISTINCT a.transportation_mode) DESC
            LIMIT 10;
        """
        
        return self.db.query(query)