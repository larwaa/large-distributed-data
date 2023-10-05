from database import Database
from timed import timed
from typing import NoReturn

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
    

    @timed
    def task6(self) -> NoReturn:
        raise NotImplementedError()
    
    @timed
    def task7a(self):
        query = """
            SELECT COUNT(DISTINCT user_id) as '# Users With Overnight Activities'
            FROM Activities
            WHERE DATEDIFF(end_datetime, start_datetime) = 1;
        """
        return self.db.query(query)
    
    @timed 
    def task7b(self):
        query = """
            SELECT transportation_mode AS 'Transportation Mode', user_id AS UserId, TIMEDIFF(end_datetime, start_datetime) as Duration
            FROM Activities
            WHERE DATEDIFF(end_datetime, start_datetime) = 1;
        """
        return self.db.query(query)

    
    @timed
    def task8(self):
        query = """
            SELECT tp1.id AS FirstId, tp2.id AS SecondId, tp1.datetime AS FirstDatetime, tp2.datetime AS SecondDatetime, ABS(TIME_TO_SEC(TIMEDIFF(tp1.datetime, tp2.datetime))) AS 'Δ Time (s)', ST_DISTANCE_SPHERE(POINT(tp1.longitude, tp1.latitude), POINT(tp2.longitude, tp2.latitude), 6378000) AS 'Distance (m)'
            FROM TrackPoints AS tp1
            CROSS JOIN TrackPoints AS tp2
            -- Exclude matching on trackpoints from the same activity
            WHERE tp1.activity_id != tp2.activity_id
                -- Exclude matching on identical trackpoints
                AND tp1.id != tp2.id
                -- Find all track points that are close in time
                AND ABS(TIME_TO_SEC(TIMEDIFF(tp1.datetime, tp2.datetime))) <= 30
                -- Out of these, find track points that are witihin 50 metres of each other
                AND ST_DISTANCE_SPHERE(POINT(tp1.longitude, tp1.latitude), POINT(tp2.longitude, tp2.latitude), 6378000) <= 50
        """
        return self.db.query(query)
    
    @timed
    def task9(self) -> NoReturn:
        raise NotImplementedError()
    
    @timed
    def task10(self) -> NoReturn:
        raise NotImplementedError()
    
    @timed
    def task11(self) -> NoReturn:
        raise NotImplementedError()
    
    @timed
    def task12(self) -> NoReturn:
        raise NotImplementedError()
    