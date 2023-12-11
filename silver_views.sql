-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `silver_views` TABLE
drop table if exists viethngn_homework.views_silver;
create table viethngn_homework.views_silver
    with (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://nguyen-viet-ceu2023/datalake/views_silver/'
        ) as
select article,
       views,
       rank,
       "date"
from viethngn_homework.bronze_views;
