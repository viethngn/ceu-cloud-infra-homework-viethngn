-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `gold_allviews` TABLE
drop table if exists viethngn_homework.gold_allviews;
create table viethngn_homework.gold_allviews
    with (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://nguyen-viet-ceu2023/datalake/gold_allviews/'
        ) as
select article,
       sum(views) as total_top_view,
       min(rank) as top_rank,
       count(distinct "date") as ranked_days
from viethngn_homework.views_silver
group by article;