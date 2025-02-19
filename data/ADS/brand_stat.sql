--@exclude_input=dw_user_item_click_log
--@exclude_input=dw_user_item_collect_log
--@exclude_input=dw_user_item_cart_log
--@exclude_input=dw_user_item_buy_log
--@exclude_input=
--@exclude_input=
--@exclude_input=
--@exclude_input=
--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-07 12:13:49
--********************************************************************--

--近60天内点击/加购/收藏/购买人数 （而非次数，剔除恶意刷单提升活跃度的行为）
--反映品牌的受欢迎程度

CREATE TABLE if not EXISTS brand_stat_feature_ads (
    brand_id string,
    click_num bigint,
    collect_num bigint,
    cart_num bigint,
    buy_num bigint
) PARTITIONED BY (ds STRING ) LIFECYCLE 90;

INSERT OVERWRITE TABLE brand_stat_feature_ads PARTITION (ds=${bizdate})
SELECT t1.brand_id, click_num, 
    if(t2.collect_num is null, 0, t2.collect_num), 
    if(t3.cart_num is null, 0, t3.cart_num),
    if(t4.buy_num is null, 0, t4.buy_num)
FROM (
    SELECT brand_id, COUNT(DISTINCT user_id) as click_num
    FROM dw_user_item_click_log 
    WHERE ds <= ${bizdate} and ds >= to_char(dateadd(TO_DATE(${bizdate}, 'yyyymmdd'), -60, 'dd'), 'yyyymmdd')
    GROUP BY brand_id
) t1 left join( -- 以click为对齐标准

    SELECT brand_id, COUNT(DISTINCT user_id) as collect_num
    FROM dw_user_item_collect_log 
    WHERE ds <= ${bizdate} and ds >= to_char(dateadd(TO_DATE(${bizdate}, 'yyyymmdd'), -60, 'dd'), 'yyyymmdd')
    GROUP BY brand_id
) t2 on t1.brand_id = t2.brand_id left join(

    SELECT brand_id, COUNT(DISTINCT user_id) as cart_num
    FROM dw_user_item_cart_log 
    WHERE ds <= ${bizdate} and ds >= to_char(dateadd(TO_DATE(${bizdate}, 'yyyymmdd'), -60, 'dd'), 'yyyymmdd')
    GROUP BY brand_id
) t3 on t1.brand_id = t3.brand_id left join (

    SELECT brand_id, COUNT(DISTINCT user_id) as buy_num
    FROM dw_user_item_buy_log 
    WHERE ds <= ${bizdate} and ds >= to_char(dateadd(TO_DATE(${bizdate}, 'yyyymmdd'), -60, 'dd'), 'yyyymmdd')
    GROUP BY brand_id
) t4 on t1.brand_id = t4.brand_id
;

SELECT count(*) from brand_stat_feature_ads where ds=20130916 ;
