CREATE TABLE IF NOT EXISTS TrackPoint (
    -- ID of the track point
    id INT NOT NULL,
    PRIMARY KEY (id),
    -- Foreign key reference to the activity which this track point belongs to
    activity_id INT,
    FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE,
    -- Latitude of the track point
    latitude DOUBLE,
    -- Longitude of the track point (note: long is a reserved keyword)
    longitude DOUBLE,
    -- Altitude of the track point
    altitude INT,
    -- Number of days (with fractional part) that have passed since 12/30/1899.
    date_days DOUBLE,
    -- Datetime of the track point, same information as expressed in date_days
    datetime DATETIME
);