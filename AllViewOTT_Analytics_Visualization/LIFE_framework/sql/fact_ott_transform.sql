select
stage_layer_sk,
ott_platform,
type,
title,
director,
"cast",
country,
date_added,
release_year,
duration_min,
duration_season as num_seasons,
rating,
categories,
description
from
(
select *, 'amazon_prime' ott_platform, amazon_prime_sk stage_layer_sk from project_analytics.stage.amazon_prime 
union
select *, 'disney_plus' ott_platform, disney_plus_sk stage_layer_sk from project_analytics.stage.disney_plus 
union
select *, 'hulu' ott_platform, hulu_sk stage_layer_sk from project_analytics.stage.hulu 
union
select *, 'netflix' ott_platform, netflix_sk stage_layer_sk from project_analytics.stage.netflix 
) a