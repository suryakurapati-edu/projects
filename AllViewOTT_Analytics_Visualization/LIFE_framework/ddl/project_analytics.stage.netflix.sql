CREATE TABLE project_analytics.stage.netflix (
    netflix_sk      UUID PRIMARY KEY,
    show_id         VARCHAR UNIQUE NOT NULL,
    type            VARCHAR,
    title           VARCHAR,
    director        VARCHAR,
    "cast"            VARCHAR,
    country         VARCHAR,
    date_added      TIMESTAMP,
    release_year    INT,
    duration_min    INT,
    duration_season INT,
    rating          VARCHAR,
    categories      VARCHAR,
    description     VARCHAR,
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);