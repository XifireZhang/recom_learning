--@exclude_input=brand_cate2_dim
--@exclude_input=dw_user_item_buy_log
--@exclude_input=brand_cate2_dim
--@exclude_input=dw_user_item_cart_log
--@exclude_input=brand_cate2_dim
--@exclude_input=dw_user_item_collect_log
--@exclude_input=brand_cate2_dim
--@exclude_input=dw_user_item_click_log
--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-07 19:01:47
--********************************************************************--

CREATE TABLE IF not EXISTS user_brand_cate2_cross_beh_feature_ads(
    user_id string, 
    brand_id string,

    click_item_90d bigint,
    click_item_60d bigint,
    click_item_30d bigint,
    click_item_15d bigint,
    click_item_7d bigint,
    click_item_3d bigint,
    click_item_1d bigint,

    click_num_90d bigint,
    click_num_60d bigint,
    click_num_30d bigint,
    click_num_15d bigint,
    click_num_7d bigint,
    click_num_3d bigint,
    click_num_1d bigint,

    click_day_90d bigint,
    click_day_60d bigint,
    click_day_30d bigint,
    click_day_15d bigint,
    click_day_7d bigint,
    click_day_3d bigint,
    click_day_1d bigint,

    collect_item_90d bigint,
    collect_item_60d bigint,
    collect_item_30d bigint,
    collect_item_15d bigint,
    collect_item_7d bigint,
    collect_item_3d bigint,
    collect_item_1d bigint,

    cart_item_90d bigint,
    cart_item_60d bigint,
    cart_item_30d bigint,
    cart_item_15d bigint,
    cart_item_7d bigint,
    cart_item_3d bigint,
    cart_item_1d bigint,

    cart_num_90d bigint,
    cart_num_60d bigint,
    cart_num_30d bigint,
    cart_num_15d bigint,
    cart_num_7d bigint,
    cart_num_3d bigint,
    cart_num_1d bigint,

    buy_item_90d bigint,
    buy_item_60d bigint,
    buy_item_30d bigint,
    buy_item_15d bigint,
    buy_item_7d bigint,
    buy_item_3d bigint,
    buy_item_1d bigint,

    buy_num_90d bigint,
    buy_num_60d bigint,
    buy_num_30d bigint,
    buy_num_15d bigint,
    buy_num_7d bigint,
    buy_num_3d bigint,
    buy_num_1d bigint
) PARTITIONED BY (ds STRING ) LIFECYCLE 90;


insert OVERWRITE table  user_brand_cate2_cross_beh_feature_ads partition (ds=${bizdate})
SELECT t1.user_id, t1.brand_id,
    click_item_90d ,
    click_item_60d ,
    click_item_30d ,
    click_item_15d ,
    click_item_7d ,
    click_item_3d ,
    click_item_1d ,

    click_num_90d ,
    click_num_60d ,
    click_num_30d ,
    click_num_15d ,
    click_num_7d ,
    click_num_3d ,
    click_num_1d ,

    click_day_90d ,
    click_day_60d ,
    click_day_30d ,
    click_day_15d ,
    click_day_7d ,
    click_day_3d ,
    click_day_1d ,

    if(collect_item_90d is null, 0, collect_item_90d),
    if(collect_item_60d is null, 0, collect_item_60d),
    if(collect_item_30d is null, 0, collect_item_30d),
    if(collect_item_15d is null, 0, collect_item_15d),
    if(collect_item_7d is null, 0, collect_item_7d),
    if(collect_item_3d is null, 0, collect_item_3d),
    if(collect_item_1d is null, 0, collect_item_1d),

    if(cart_item_90d is null, 0, cart_item_90d),
    if(cart_item_60d is null, 0, cart_item_60d),
    if(cart_item_30d is null, 0, cart_item_30d),
    if(cart_item_15d is null, 0, cart_item_15d),
    if(cart_item_7d is null, 0, cart_item_7d),
    if(cart_item_3d is null, 0, cart_item_3d),
    if(cart_item_1d is null, 0, cart_item_1d),

    if(cart_num_90d is null, 0, cart_num_90d),
    if(cart_num_60d is null, 0, cart_num_60d),
    if(cart_num_30d is null, 0, cart_num_30d),
    if(cart_num_15d is null, 0, cart_num_15d),
    if(cart_num_7d is null, 0, cart_num_7d),
    if(cart_num_3d is null, 0, cart_num_3d),
    if(cart_num_1d is null, 0, cart_num_1d),

    if(buy_item_90d is null, 0, buy_item_90d),
    if(buy_item_60d is null, 0, buy_item_60d),
    if(buy_item_30d is null, 0, buy_item_30d),
    if(buy_item_15d is null, 0, buy_item_15d),
    if(buy_item_7d is null, 0, buy_item_7d),
    if(buy_item_3d is null, 0, buy_item_3d),
    if(buy_item_1d is null, 0, buy_item_1d),

    if(buy_num_90d is null, 0, buy_num_90d),
    if(buy_num_60d is null, 0, buy_num_60d),
    if(buy_num_30d is null, 0, buy_num_30d),
    if(buy_num_15d is null, 0, buy_num_15d),
    if(buy_num_7d is null, 0, buy_num_7d),
    if(buy_num_3d is null, 0, buy_num_3d),
    if(buy_num_1d is null, 0, buy_num_1d)
FROM (
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as click_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as click_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as click_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as click_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as click_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as click_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as click_item_1d

        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), 1, 0)) as click_num_90d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), 1, 0)) as click_num_60d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), 1, 0)) as click_num_30d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), 1, 0)) as click_num_15d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), 1, 0)) as click_num_7d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), 1, 0)) as click_num_3d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), 1, 0)) as click_num_1d

        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), ds, null)) as click_day_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), ds, null)) as click_day_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), ds, null)) as click_day_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), ds, null)) as click_day_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), ds, null)) as click_day_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), ds, null)) as click_day_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), ds, null)) as click_day_1d


    FROM (
        SELECT t1.user_id, t2.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, cate2, item_id, ds
            FROM dw_user_item_click_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id, cate2
            from brand_cate2_dim 
        ) t2 on t1.cate2 = t2.cate2
    ) t1 GROUP BY user_id, brand_id
) t1 left JOIN (
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_1d
    FROM (
        SELECT t1.user_id, t2.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, cate2, item_id, ds
            FROM dw_user_item_collect_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id, cate2
            from brand_cate2_dim 
        ) t2 on t1.cate2 = t2.cate2
    ) t1 GROUP BY user_id, brand_id
) t2 on t1.user_id=t2.user_id and t1.brand_id=t2.brand_id
left join(
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_1d

        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_90d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_60d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_30d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_15d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_7d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_3d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_1d


    FROM (
        SELECT t1.user_id, t2.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, cate2, item_id, ds
            FROM dw_user_item_cart_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id, cate2
            from brand_cate2_dim 
        ) t2 on t1.cate2 = t2.cate2
    ) t1 GROUP BY user_id, brand_id
) t3 on t1.user_id=t3.user_id and t1.brand_id = t3.brand_id
left join(
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_1d

        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_90d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_60d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_30d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_15d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_7d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_3d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_1d


    FROM (
        SELECT t1.user_id, t2.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, cate2, item_id, ds
            FROM dw_user_item_buy_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id, cate2
            from brand_cate2_dim 
        ) t2 on t1.cate2 = t2.cate2
    ) t1 GROUP BY user_id, brand_id
) t4 on t1.user_id=t4.user_id and t1.brand_id = t4.brand_id
;


select count(*) from user_brand_cate2_cross_beh_feature_ads where ds=20130916 ;

select count(distinct brand_id) from user_brand_cate2_cross_beh_feature_ads where ds=20130916 ;


--@exclude_input=brand_top500_buy_num
--@exclude_input=dw_user_item_buy_log
--@exclude_input=brand_top500_buy_num
--@exclude_input=dw_user_item_cart_log
--@exclude_input=brand_top500_buy_num
--@exclude_input=dw_user_item_collect_log
--@exclude_input=brand_top500_buy_num
--@exclude_input=dw_user_item_click_log
--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-07 14:08:54
--********************************************************************--

--交互表，
--在品牌上的特征，1/3/7/15/30/60/90 次数/商品数/天数
--在品牌top3二级行业的特征  

CREATE TABLE IF not EXISTS user_brand_cross_beh_feature_ads(
    user_id string, 
    brand_id string,

    click_item_90d bigint,
    click_item_60d bigint,
    click_item_30d bigint,
    click_item_15d bigint,
    click_item_7d bigint,
    click_item_3d bigint,
    click_item_1d bigint,

    click_num_90d bigint,
    click_num_60d bigint,
    click_num_30d bigint,
    click_num_15d bigint,
    click_num_7d bigint,
    click_num_3d bigint,
    click_num_1d bigint,

    click_day_90d bigint,
    click_day_60d bigint,
    click_day_30d bigint,
    click_day_15d bigint,
    click_day_7d bigint,
    click_day_3d bigint,
    click_day_1d bigint,

    collect_item_90d bigint,
    collect_item_60d bigint,
    collect_item_30d bigint,
    collect_item_15d bigint,
    collect_item_7d bigint,
    collect_item_3d bigint,
    collect_item_1d bigint,

    cart_item_90d bigint,
    cart_item_60d bigint,
    cart_item_30d bigint,
    cart_item_15d bigint,
    cart_item_7d bigint,
    cart_item_3d bigint,
    cart_item_1d bigint,

    cart_num_90d bigint,
    cart_num_60d bigint,
    cart_num_30d bigint,
    cart_num_15d bigint,
    cart_num_7d bigint,
    cart_num_3d bigint,
    cart_num_1d bigint,

    buy_item_90d bigint,
    buy_item_60d bigint,
    buy_item_30d bigint,
    buy_item_15d bigint,
    buy_item_7d bigint,
    buy_item_3d bigint,
    buy_item_1d bigint,

    buy_num_90d bigint,
    buy_num_60d bigint,
    buy_num_30d bigint,
    buy_num_15d bigint,
    buy_num_7d bigint,
    buy_num_3d bigint,
    buy_num_1d bigint
) PARTITIONED BY (ds STRING ) LIFECYCLE 90;


insert OVERWRITE table  user_brand_cross_beh_feature_ads partition (ds=${bizdate})
SELECT t1.user_id, t1.brand_id,
    click_item_90d ,
    click_item_60d ,
    click_item_30d ,
    click_item_15d ,
    click_item_7d ,
    click_item_3d ,
    click_item_1d ,

    click_num_90d ,
    click_num_60d ,
    click_num_30d ,
    click_num_15d ,
    click_num_7d ,
    click_num_3d ,
    click_num_1d ,

    click_day_90d ,
    click_day_60d ,
    click_day_30d ,
    click_day_15d ,
    click_day_7d ,
    click_day_3d ,
    click_day_1d ,

    if(collect_item_90d is null, 0, collect_item_90d),
    if(collect_item_60d is null, 0, collect_item_60d),
    if(collect_item_30d is null, 0, collect_item_30d),
    if(collect_item_15d is null, 0, collect_item_15d),
    if(collect_item_7d is null, 0, collect_item_7d),
    if(collect_item_3d is null, 0, collect_item_3d),
    if(collect_item_1d is null, 0, collect_item_1d),

    if(cart_item_90d is null, 0, cart_item_90d),
    if(cart_item_60d is null, 0, cart_item_60d),
    if(cart_item_30d is null, 0, cart_item_30d),
    if(cart_item_15d is null, 0, cart_item_15d),
    if(cart_item_7d is null, 0, cart_item_7d),
    if(cart_item_3d is null, 0, cart_item_3d),
    if(cart_item_1d is null, 0, cart_item_1d),

    if(cart_num_90d is null, 0, cart_num_90d),
    if(cart_num_60d is null, 0, cart_num_60d),
    if(cart_num_30d is null, 0, cart_num_30d),
    if(cart_num_15d is null, 0, cart_num_15d),
    if(cart_num_7d is null, 0, cart_num_7d),
    if(cart_num_3d is null, 0, cart_num_3d),
    if(cart_num_1d is null, 0, cart_num_1d),

    if(buy_item_90d is null, 0, buy_item_90d),
    if(buy_item_60d is null, 0, buy_item_60d),
    if(buy_item_30d is null, 0, buy_item_30d),
    if(buy_item_15d is null, 0, buy_item_15d),
    if(buy_item_7d is null, 0, buy_item_7d),
    if(buy_item_3d is null, 0, buy_item_3d),
    if(buy_item_1d is null, 0, buy_item_1d),

    if(buy_num_90d is null, 0, buy_num_90d),
    if(buy_num_60d is null, 0, buy_num_60d),
    if(buy_num_30d is null, 0, buy_num_30d),
    if(buy_num_15d is null, 0, buy_num_15d),
    if(buy_num_7d is null, 0, buy_num_7d),
    if(buy_num_3d is null, 0, buy_num_3d),
    if(buy_num_1d is null, 0, buy_num_1d)
FROM (
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as click_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as click_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as click_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as click_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as click_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as click_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as click_item_1d

        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), 1, 0)) as click_num_90d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), 1, 0)) as click_num_60d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), 1, 0)) as click_num_30d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), 1, 0)) as click_num_15d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), 1, 0)) as click_num_7d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), 1, 0)) as click_num_3d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), 1, 0)) as click_num_1d

        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), ds, null)) as click_day_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), ds, null)) as click_day_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), ds, null)) as click_day_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), ds, null)) as click_day_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), ds, null)) as click_day_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), ds, null)) as click_day_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), ds, null)) as click_day_1d


    FROM (
        SELECT t1.user_id, t2.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, item_id, ds
            FROM dw_user_item_click_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id
            from brand_top500_buy_num 
        ) t2 on t1.brand_id = t2.brand_id
    ) t1 GROUP BY user_id, brand_id
) t1 left JOIN (
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as collect_item_1d
    FROM (
        SELECT t1.user_id, t2.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, item_id, ds
            FROM dw_user_item_collect_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id
            from brand_top500_buy_num 
        ) t2 on t1.brand_id = t2.brand_id
    ) t1 GROUP BY user_id, brand_id
) t2 on t1.user_id=t2.user_id and t1.brand_id=t2.brand_id
left join(
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as cart_item_1d

        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_90d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_60d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_30d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_15d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_7d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_3d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), 1, 0)) as cart_num_1d


    FROM (
        SELECT t1.user_id, t2.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, item_id, ds
            FROM dw_user_item_cart_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id
            from brand_top500_buy_num 
        ) t2 on t1.brand_id = t2.brand_id
    ) t1 GROUP BY user_id, brand_id
) t3 on t1.user_id=t3.user_id and t1.brand_id = t3.brand_id
left join(
    SELECT user_id, brand_id
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_90d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_60d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_30d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_15d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_7d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_3d
        ,count(distinct if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), item_id, null)) as buy_item_1d

        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_90d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-60, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_60d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-30, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_30d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-15, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_15d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-7, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_7d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-3, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_3d
        ,SUM( if(ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-1, 'dd'),'yyyymmdd'), 1, 0)) as buy_num_1d


    FROM (
        SELECT t1.user_id, t1.brand_id, t1.item_id, t1.ds
        FROM (
            SELECT user_id, brand_id, item_id, ds
            FROM dw_user_item_buy_log 
            WHERE ds<=${bizdate} and ds>=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'),-90, 'dd'),'yyyymmdd')
        ) t1 join (
            SELECT brand_id
            from brand_top500_buy_num 
        ) t2 on t1.brand_id = t2.brand_id
    ) t1 GROUP BY user_id, brand_id
) t4 on t1.user_id=t4.user_id and t1.brand_id = t4.brand_id
;


SELECT * from user_brand_cross_beh_feature_ads where ds=20130916 limit 4;
select count(DISTINCT brand_id) from user_brand_cross_beh_feature_ads where ds=20130916 ;
