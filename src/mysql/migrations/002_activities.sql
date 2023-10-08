CREATE TABLE IF NOT EXISTS Activities (
    -- ID of the activity
    id VARCHAR(50) NOT NULL,
    PRIMARY KEY (id),
    -- Foreign key reference to the user which this activity belongs to
    user_id VARCHAR(50),
    FOREIGN KEY (user_id) REFERENCES Users(id) ON DELETE CASCADE,
    -- Type of transportation. NOT NULL to avoid having multiple empty states, i.e. null and ""
    -- Only for users with has_labels=True
    transportation_mode VARCHAR(255) NOT NULL DEFAULT "",
    -- Start datetime for this activity
    start_datetime DATETIME,
    INDEX (start_datetime),
    -- End datetime for this activity
    end_datetime DATETIME,
    INDEX (end_datetime)
);