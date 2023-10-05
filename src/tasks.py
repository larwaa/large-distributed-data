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

        statement = """
        SELECT
            (SELECT Count(*) AS UsersCount FROM Users) AS '# Users',
            (SELECT Count(*) AS UsersCount FROM Activities) AS '# Activities',
            (SELECT Count(*) AS UsersCount FROM TrackPoints) AS '# TrackPoints';
        """


        return self.db.query(statement)
    
    @timed
    def task2(self):
        statement = """
            SELECT CAST(ROUND(AVG(count), 0) AS SIGNED) AS Avg, MAX(count) AS Max, MIN(count) AS Min
            FROM (
                SELECT COUNT(*) AS count
                FROM TrackPoints as tp
                LEFT JOIN Activities as a
                    ON tp.activity_id = a.id
                GROUP BY a.user_id
            ) as counts;
        """

        return self.db.query(statement)
    
    @timed
    def task3(self):
        statement = """
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
        return self.db.query(statement)
    
    @timed
    def task4(self):
        statement = """
            SELECT DISTINCT u.id AS UserId
            FROM Users AS u
            LEFT JOIN Activities AS a
                ON u.id = a.user_id
            WHERE a.transportation_mode LIKE 'Bus';
        """

        return self.db.query(statement)
    
    @timed
    def task5(self):
        statement = """
            SELECT DISTINCT u.id AS UserID, Count(DISTINCT a.transportation_mode) as '# Transportation Modes', GROUP_CONCAT(DISTINCT a.transportation_mode SEPARATOR ', ') AS 'Transportation Modes'
            FROM Activities AS a
            LEFT JOIN Users AS u
                ON a.user_id = u.id
            WHERE a.transportation_mode != ""
            GROUP BY u.id
            ORDER BY Count(DISTINCT a.transportation_mode) DESC
            LIMIT 10;
        """
        
        return self.db.query(statement)
    

    @timed
    def task6(self):
        statement = """
            SELECT a1.id as activity1_id, a2.id AS activity2_id
            FROM Activities AS a1
            JOIN Activities AS a2
            ON a1.id < a2.id
            AND a1.start_datetime = a2.start_datetime
            AND a1.end_datetime = a2.end_datetime;
        """
        return self.db.query(statement)
    
    @timed
    def task7a(self):
        statement = """
            SELECT COUNT(DISTINCT user_id) as '# Users With Overnight Activities'
            FROM Activities
            WHERE DATEDIFF(end_datetime, start_datetime) = 1;
        """
        return self.db.query(statement)
    
    @timed 
    def task7b(self):
        statement = """
            SELECT transportation_mode AS 'Transportation Mode', user_id AS UserId, TIMEDIFF(end_datetime, start_datetime) as Duration
            FROM Activities
            WHERE DATEDIFF(end_datetime, start_datetime) = 1;
        """
        return self.db.query(statement)

    
    @timed
    def task8(self):
        statement = """
            WITH user_pairs AS (
                SELECT DISTINCT a1.user_id AS user_id1, a2.user_id AS user_id2
                FROM Activities a1
                -- Make a combination of all activities
                JOIN Activities a2 ON a1.id < a2.id
                    -- We restrict the search space to activities that overlap with a 30 second margin
                    -- to limit the number of track point comparisons that we have to perform.
                    -- Activities that do not overlap within at least a 30 second margin
                    -- should not contain track points that are within 30 seconds of each other.
                    AND TIME_TO_SEC(TIMEDIFF(a2.start_datetime, a1.end_datetime)) <= 30
                    AND TIME_TO_SEC(TIMEDIFF(a1.start_datetime, a2.end_datetime)) <= 30
                    -- Avoid comparing a user to themselves
                    AND a1.user_id < a2.user_id
                -- Join in the track points on the two sets of activities
                JOIN TrackPoints p1 ON p1.activity_id = a1.id
                JOIN TrackPoints p2 ON p2.activity_id = a2.id
                -- Then, after restricting the search space, we check for
                -- track points that are close both in time
                WHERE ABS(TIME_TO_SEC(TIMEDIFF(p1.datetime, p2.datetime))) <= 30
                -- and in space
                AND ST_Distance_Sphere(p1.geom, p2.geom) <= 50
            )
            -- Finally, we select the list of distinct user_ids of users
            -- who have been near other users in space and time
            SELECT DISTINCT user_id
            FROM (
                -- Combine the two columns of user ID pairs into a single column of user IDs
                -- to find the total count of unique users who have been near others
                SELECT user_id1 AS user_id FROM user_pairs
                UNION
                SELECT user_id2 FROM user_pairs
            ) AS user_ids
            -- Order the results by ascending ID
            ORDER BY user_id ASC;
        """
        return self.db.query(statement)
    
    @timed
    def task9(self):
        """
        Find the top 15 users who have gained the most altitude meters.

        Output should be a table with (id, total meters gained per user).
        Remember that some altitude-values are invalid

        Interpreting this as total gain, which means we don't subtract altitude
        when the user is descending, only add it when they're ascending.
        """
        statement = """
            SELECT a1.user_id, SUM(tp2.altitude - tp1.altitude) AS 'Altitude Gain'
            FROM TrackPoints tp1
            JOIN TrackPoints tp2 ON tp2.id = tp1.id + 1
                AND tp2.altitude > tp1.altitude
                AND tp1.activity_id = tp2.activity_id
            JOIN Activities a1 ON a1.id = tp1.activity_id
            GROUP BY a1.user_id
            ORDER BY SUM(tp2.altitude - tp1.altitude) DESC
            LIMIT 15;
        """
        return self.db.query(statement)
    
    @timed
    def task10(self):
        """
        Find the users that have traveled the longest total distance in one day for each transportation mode.

        TODO:
        Refine distance calculations
        Should include:
        - distance covered by multiple activities in the same day
        - distance covered by activities that span multiple days, but where you include the travel time up until the end of the day (?)
        
        """
        statement = """
            WITH distances AS (
                SELECT a1.user_id AS user_id, a1.transportation_mode AS transportation_mode, ROUND(SUM(ST_DISTANCE_SPHERE(tp1.geom, tp2.geom)) / 1000, 2) AS distance
                FROM Activities a1
                JOIN TrackPoints tp1 ON tp1.activity_id = a1.id
                JOIN TrackPoints tp2 ON tp2.id = tp1.id + 1 
                    AND tp1.activity_id = tp2.activity_id
                WHERE a1.transportation_mode != ""
                AND DATE(a1.start_datetime) = DATE(a1.end_datetime)
                GROUP BY a1.user_id, a1.transportation_mode, a1.id
                ORDER BY distance DESC
            )
            SELECT max.transportation_mode AS 'Transportation Mode', max.distance AS 'Max Distance (km)', MAX(d2.user_id) AS UserID
            FROM (
                SELECT d1.transportation_mode, MAX(d1.distance) AS distance
                FROM distances d1
                GROUP BY d1.transportation_mode
            ) AS max
            LEFT JOIN distances d2 ON d2.distance = max.distance
            GROUP BY max.transportation_mode;
        """

        return self.db.query(statement)
    
    @timed
    def task11(self):
        """
        Find all users who have invalid activities, and the number of invalid activities per user
        An invalid activity is defined as an activity with consecutive trackpoints where the timestamps
        deviate with at least 5 minutes.
        """
        statement = """
            SELECT a1.user_id as UserID, COUNT(DISTINCT a1.id) as '# Invalid Activities'
            FROM Activities a1
            JOIN TrackPoints p1 ON a1.id = p1.activity_id
            JOIN TrackPoints p2 ON p2.id = p1.id + 1
                AND p2.activity_id = p1.activity_id
            WHERE ABS(TIME_TO_SEC(TIMEDIFF(p1.datetime, p2.datetime))) >= 5 * 60
            GROUP BY a1.user_id
            ORDER BY COUNT(DISTINCT a1.id) DESC;
        """

        return self.db.query(statement)
    
    @timed
    def task12(self):
        """
        Find all users who have registered transportation_mode and their most used transportation_mode.
        The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id
        Some users may have the same number of activities tagged with e.g. walk and car. In this case it is up to you 
        to decide which transportation mode to include in your answer (choose one).

        Tie breaks: reverse lexicographical order because walking is the best :)
        """

        statement = """
        WITH counts AS (
            SELECT user_id, transportation_mode, COUNT(transportation_mode) as count
            FROM Activities
            WHERE transportation_mode != ""
            GROUP BY user_id, transportation_mode
            ORDER BY user_id, COUNT(transportation_mode) DESC
        )
        SELECT max.user_id AS user_id, MAX(c2.transportation_mode) AS most_used_transportation_mode
        FROM (
            SELECT c1.user_id, MAX(c1.count) AS count
            FROM counts c1
            GROUP BY c1.user_id
        ) AS max
        LEFT JOIN counts c2 ON max.count = c2.count
        GROUP BY max.user_id;
        """

        return self.db.query(statement)
    