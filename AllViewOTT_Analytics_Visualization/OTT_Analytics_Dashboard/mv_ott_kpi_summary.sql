-- REFRESH MATERIALIZED VIEW project_analytics.consumption.mv_ott_kpi_summary;
-- drop MATERIALIZED VIEW project_analytics.consumption.mv_ott_kpi_summary;
CREATE MATERIALIZED VIEW project_analytics.consumption.mv_ott_kpi_summary AS
WITH base AS (
    SELECT
        ott_platform,
        "type",
        duration_min,
        num_seasons,
        rating,
        categories,
        director,
        country,
        release_year,
        date_added::date
    FROM project_analytics.processed.fact_ott
)
SELECT
    ott_platform,

    COUNT(*) AS total_titles,

    SUM(CASE WHEN type = 'Movie' THEN 1 ELSE 0 END) AS total_movies,
    SUM(CASE WHEN type = 'TV Show' THEN 1 ELSE 0 END) AS total_tv_shows,

    AVG(CASE WHEN type = 'Movie' THEN duration_min ELSE NULL END) AS avg_movie_duration_min,
    AVG(CASE WHEN type = 'TV Show' THEN num_seasons ELSE NULL END) AS avg_tvshow_seasons,

    COUNT(DISTINCT director) AS unique_directors,
    COUNT(DISTINCT country) AS unique_countries,

    date_trunc('month', date_added) AS content_added_month,
    release_year

FROM base
GROUP BY ott_platform, content_added_month, release_year