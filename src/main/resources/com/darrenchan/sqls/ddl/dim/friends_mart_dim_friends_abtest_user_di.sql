CREATE TABLE IF NOT EXISTS ${schema}.friends_mart_dim_friends_abtest_user_di
(
    user_id bigint comment 'user id',
    abtest_group string comment 'User abtest group',
    max_event_timestamp bigint comment '最晚出现时间'
)
partitioned by (tz_type STRING, grass_region STRING, grass_date date)
stored as parquet
tblproperties ('parquet.compression'='SNAPPY')