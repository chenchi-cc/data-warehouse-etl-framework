insert overwrite table ${schema}.friends_mart_dim_friends_abtest_user_di partition(tz_type='local', grass_region='${grassRegion}', grass_date='${grassDate}')
select  /*+REPARTITION(50)*/
    user_id,
    abtest_group,
    max(event_timestamp) max_event_timestamp
from(
        select
            user_id,
            event_timestamp,
            abtest_head
        from ${schema}.friends_mart_dwd_traffic_log_di
        where grass_date = cast('${grassDate}' as date) and grass_region = '${grassRegion}'
          and abtest_head is not null and abtest_head != ''
    ) a
lateral view explode(split_head_abtest_info(abtest_head)) adTable AS abtest_group
group by user_id, abtest_group