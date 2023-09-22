CREATE TABLE IF NOT EXISTS User (
    -- ID of users
    id VARCHAR(50) NOT NULL,
    PRIMARY KEY (id),
    -- If the user labeled their transportation data with a transportation mode
    has_labels BOOLEAN DEFAULT false
);