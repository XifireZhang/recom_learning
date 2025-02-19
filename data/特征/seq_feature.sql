--@exclude_input=dw_user_item_buy_log
--@exclude_input=brand_top10000_buy_num
--@exclude_input=dw_user_item_collect_log
--@exclude_input=brand_top10000_buy_num
--@exclude_input=dw_user_item_click_log
--@exclude_input=
--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-11 17:33:24
--********************************************************************--
CREATE table if not EXISTS user_click_brand_seq_feature(
    user_id string,
    brand_id_seq  string
)PARTITIONED BY (ds string) LIFECYCLE 90;

insert OVERWRITE TABLE user_click_brand_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('b_',brand_id)) as brand_id_seq
from (
    select t2.user_id, t2.brand_id
    from (
        select brand_id
        from brand_top10000_buy_num
    )t1 join (
        SELECT user_id,brand_id
            , ROW_NUMBER() OVER(PARTITION BY user_id  ORDER BY op_time desc) AS number
        from (
            select user_id, brand_id, max(op_time) as op_time
            from dw_user_item_click_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
                and brand_id is not null
            group by user_id, brand_id
        )t1
    )t2 on t1.brand_id=t2.brand_id
    where number<=50
)t1 group by user_id
;

SELECT count(*) from user_click_brand_seq_feature where ds = 20130916 ;

create TABLE if not exists user_collect_brand_seq_feature (
    user_id string,
    brand_id_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_collect_brand_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('b_',brand_id)) as brand_id_seq
from (
    select t2.user_id, t2.brand_id
    from (
        select brand_id
        from brand_top10000_buy_num
    )t1 join (
        select user_id, brand_id
            , ROW_NUMBER() OVER(PARTITION BY user_id  ORDER BY op_time desc) AS number
        from (
            select user_id, brand_id, max(op_time) as op_time
            from dw_user_item_collect_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
                and brand_id is not null
            group by user_id, brand_id
        )t1
    )t2 on t1.brand_id=t2.brand_id
    where number<=50
)t1 group by user_id
;

create TABLE if not exists user_pay_brand_seq_feature (
    user_id string,
    brand_id_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_brand_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('b_',brand_id)) as brand_id_seq
from (
    select t2.user_id, t2.brand_id
    from (
        select brand_id
        from brand_top10000_buy_num
    )t1 join (
        select user_id, brand_id,
            ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC ) AS number
        from (
            select user_id, brand_id, max(op_time) as op_time
            from dw_user_item_buy_log
            where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
                and brand_id is not null
            group by user_id, brand_id
        )t1
    )t2 on t1.brand_id=t2.brand_id
    where number<=50
)t1 group by user_id
;

--@exclude_input=dw_user_item_buy_log
--@exclude_input=dw_user_item_collect_log
--@exclude_input=dw_user_item_click_log
--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-11 20:39:45
--********************************************************************--
--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-11 17:33:24
--********************************************************************--
create TABLE if not exists user_click_cate_seq_feature (
    user_id string,
    cate_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_click_cate_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('c_',cate2)) as cate2_seq
from (
    select user_id, cate2
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC) AS number
    from (
        select user_id, cate2, max(op_time) as op_time
        from dw_user_item_click_log
        where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
            and cate2 is not null
        group by user_id, cate2
    )t1
)t1
where number<=50
group by user_id
;

SELECT * from user_click_cate_seq_feature  where ds = 20130916 limit 4;

create TABLE if not exists user_collect_cate_seq_feature (
    user_id string,
    cate_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_collect_cate_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('c_',cate2)) as cate2_seq
from (
    select user_id, cate2
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC) AS number
    from (
        select user_id, cate2, MAX(op_time) as op_time
        from dw_user_item_collect_log
        where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
            and cate2 is not null
        group by user_id, cate2
    )t1
)t1
where number<=50
group by user_id
;

create TABLE if not exists user_pay_cate_seq_feature (
    user_id string,
    cate_seq string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_cate_seq_feature PARTITION (ds=${bizdate})
select user_id, WM_CONCAT(',', concat('c_',cate2)) as cate_seq
from (
    select user_id, cate2
        , ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY op_time DESC) AS number
    from (
        select user_id, cate2, max(op_time) as op_time
        from dw_user_item_buy_log
        where ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd')
            and cate2 is not null
        group by user_id, cate2
    )t1
)t1
where number<=50
group by user_id
;
