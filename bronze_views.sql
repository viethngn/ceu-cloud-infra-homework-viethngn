-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `bronze_views` TABLE
drop database if exists viethngn_homework;
create database viethngn_homework;

drop table if exists viethngn_homework.bronze_views;

create external table
viethngn_homework.bronze_views (
    article string,
    views int,
    rank int,
    date date,
    retrieved_at timestamp
)
row format serde 'org.openx.data.jsonserde.JsonSerDe'
location 's3://nguyen-viet-ceu2023/datalake/views/';
 