--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-08 17:03:08
--********************************************************************--
-- 评估时，缩减测试集规模，4个品牌+300w用户？
CREATE TABLE  if not EXISTS user_pay_sample_eval (
    user_id string,
    brand_id string,
    label bigint
)PARTITIONED BY (ds STRING )   LIFECYCLE 90;

INSERT OVERWRITE table user_pay_sample_eval partition (ds=${bizdate})
SELECT user_id, brand_id, max(LABEL ) as LABEL 
FROM (
 
    SELECT /*+mapjoin(t2)*/
        t1.user_id, t2.brand_id, 0 as LABEL 
    FROM (
        SELECT user_id
        FROM user_id_number 
        where number<=300000
    )t1 JOIN (

            --b47686 韩都衣舍
            --b56508 三星手机
            --b62063 诺基亚
            --b78739 LILY
            select 'b47686' as brand_id
            union all
            select 'b56508' as brand_id
            union all
            select 'b62063' as brand_id
            union all
            select 'b78739' as brand_id
    )t2

    union ALL 

    SELECT DISTINCT user_id, brand_id, 1 as LABEL 
        from dw_user_item_buy_log
        where ds>=${bizdate} and ds<=to_char(dateadd(to_date(${bizdate}, 'yyyymmdd'), 7, 'dd'),'yyyymmdd') 
            and brand_id in ('b47686', 'b56508', 'b62063', 'b78739')
) t1 GROUP BY user_id, brand_id;


SELECT * from user_pay_sample_eval where ds=20130923 limit 4;


--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-08 17:13:04
--********************************************************************--


--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-08 14:17:27
--********************************************************************--

create table if not exists user_pay_sample_feature_join_eval (
    user_id string,
    brand_id string,
    label bigint,
    bizdate string,
    rnd double,

    click_item_num_3d BIGINT,
    click_brand_num_3d BIGINT,
    click_seller_num_3d BIGINT,
    click_cate1_num_3d BIGINT,
    click_cate2_num_3d BIGINT,
    click_cnt_days_3d BIGINT,
    click_item_num_7d BIGINT,
    click_brand_num_7d BIGINT,
    click_seller_num_7d BIGINT,
    click_cate1_num_7d BIGINT,
    click_cate2_num_7d BIGINT,
    click_cnt_days_7d BIGINT,
    click_item_num_15d BIGINT,
    click_brand_num_15d BIGINT,
    click_seller_num_15d BIGINT,
    click_cate1_num_15d BIGINT,
    click_cate2_num_15d BIGINT,
    click_cnt_days_15d BIGINT,
    click_item_num_30d BIGINT,
    click_brand_num_30d BIGINT,
    click_seller_num_30d BIGINT,
    click_cate1_num_30d BIGINT,
    click_cate2_num_30d BIGINT,
    click_cnt_days_30d BIGINT,
    click_item_num_60d BIGINT,
    click_brand_num_60d BIGINT,
    click_seller_num_60d BIGINT,
    click_cate1_num_60d BIGINT,
    click_cate2_num_60d BIGINT,
    click_cnt_days_60d BIGINT,
    click_item_num_90d BIGINT,
    click_brand_num_90d BIGINT,
    click_seller_num_90d BIGINT,
    click_cate1_num_90d BIGINT,
    click_cate2_num_90d BIGINT,
    click_cnt_days_90d BIGINT,

    collect_item_num_3d BIGINT,
    collect_brand_num_3d BIGINT,
    collect_seller_num_3d BIGINT,
    collect_cate1_num_3d BIGINT,
    collect_cate2_num_3d BIGINT,
    collect_item_num_7d BIGINT,
    collect_brand_num_7d BIGINT,
    collect_seller_num_7d BIGINT,
    collect_cate1_num_7d BIGINT,
    collect_cate2_num_7d BIGINT,
    collect_item_num_15d BIGINT,
    collect_brand_num_15d BIGINT,
    collect_seller_num_15d BIGINT,
    collect_cate1_num_15d BIGINT,
    collect_cate2_num_15d BIGINT,

    collect_item_num_30d BIGINT,
    collect_brand_num_30d BIGINT,
    collect_seller_num_30d BIGINT,
    collect_cate1_num_30d BIGINT,
    collect_cate2_num_30d BIGINT,

    collect_item_num_60d BIGINT,
    collect_brand_num_60d BIGINT,
    collect_seller_num_60d BIGINT,
    collect_cate1_num_60d BIGINT,
    collect_cate2_num_60d BIGINT,

    collect_item_num_90d BIGINT,
    collect_brand_num_90d BIGINT,
    collect_seller_num_90d BIGINT,
    collect_cate1_num_90d BIGINT,
    collect_cate2_num_90d BIGINT,

    cart_item_num_3d BIGINT,
    cart_brand_num_3d BIGINT,
    cart_seller_num_3d BIGINT,
    cart_cate1_num_3d BIGINT,
    cart_cate2_num_3d BIGINT,
    cart_item_num_7d BIGINT,
    cart_brand_num_7d BIGINT,
    cart_seller_num_7d BIGINT,
    cart_cate1_num_7d BIGINT,
    cart_cate2_num_7d BIGINT,
    cart_item_num_15d BIGINT,
    cart_brand_num_15d BIGINT,
    cart_seller_num_15d BIGINT,
    cart_cate1_num_15d BIGINT,
    cart_cate2_num_15d BIGINT,

    cart_item_num_30d BIGINT,
    cart_brand_num_30d BIGINT,
    cart_seller_num_30d BIGINT,
    cart_cate1_num_30d BIGINT,
    cart_cate2_num_30d BIGINT,

    cart_item_num_60d BIGINT,
    cart_brand_num_60d BIGINT,
    cart_seller_num_60d BIGINT,
    cart_cate1_num_60d BIGINT,
    cart_cate2_num_60d BIGINT,

    cart_item_num_90d BIGINT,
    cart_brand_num_90d BIGINT,
    cart_seller_num_90d BIGINT,
    cart_cate1_num_90d BIGINT,
    cart_cate2_num_90d BIGINT,

    buy_item_num_3d BIGINT,
    buy_brand_num_3d BIGINT,
    buy_seller_num_3d BIGINT,
    buy_cate1_num_3d BIGINT,
    buy_cate2_num_3d BIGINT,
    buy_item_num_7d BIGINT,
    buy_brand_num_7d BIGINT,
    buy_seller_num_7d BIGINT,
    buy_cate1_num_7d BIGINT,
    buy_cate2_num_7d BIGINT,
    buy_item_num_15d BIGINT,
    buy_brand_num_15d BIGINT,
    buy_seller_num_15d BIGINT,
    buy_cate1_num_15d BIGINT,
    buy_cate2_num_15d BIGINT,

    buy_item_num_30d BIGINT,
    buy_brand_num_30d BIGINT,
    buy_seller_num_30d BIGINT,
    buy_cate1_num_30d BIGINT,
    buy_cate2_num_30d BIGINT,

    buy_item_num_60d BIGINT,
    buy_brand_num_60d BIGINT,
    buy_seller_num_60d BIGINT,
    buy_cate1_num_60d BIGINT,
    buy_cate2_num_60d BIGINT,

    buy_item_num_90d BIGINT,
    buy_brand_num_90d BIGINT,
    buy_seller_num_90d BIGINT,
    buy_cate1_num_90d BIGINT,
    buy_cate2_num_90d BIGINT,

    brand_click_num bigint,
    brand_collect_num bigint,
    brand_cart_num bigint,
    brand_buy_num bigint,

    cb_click_item_90d bigint,
    cb_click_item_60d bigint,
    cb_click_item_30d bigint,
    cb_click_item_15d bigint,
    cb_click_item_7d bigint,
    cb_click_item_3d bigint,
    cb_click_item_1d bigint,
    cb_click_num_90d bigint,
    cb_click_num_60d bigint,
    cb_click_num_30d bigint,
    cb_click_num_15d bigint,
    cb_click_num_7d bigint,
    cb_click_num_3d bigint,
    cb_click_num_1d bigint,
    cb_click_day_90d bigint,
    cb_click_day_60d bigint,
    cb_click_day_30d bigint,
    cb_click_day_15d bigint,
    cb_click_day_7d bigint,
    cb_click_day_3d bigint,
    cb_click_day_1d bigint,
    cb_collect_item_90d bigint,
    cb_collect_item_60d bigint,
    cb_collect_item_30d bigint,
    cb_collect_item_15d bigint,
    cb_collect_item_7d bigint,
    cb_collect_item_3d bigint,
    cb_collect_item_1d bigint,
    cb_cart_item_90d bigint,
    cb_cart_item_60d bigint,
    cb_cart_item_30d bigint,
    cb_cart_item_15d bigint,
    cb_cart_item_7d bigint,
    cb_cart_item_3d bigint,
    cb_cart_item_1d bigint,
    cb_cart_num_90d bigint,
    cb_cart_num_60d bigint,
    cb_cart_num_30d bigint,
    cb_cart_num_15d bigint,
    cb_cart_num_7d bigint,
    cb_cart_num_3d bigint,
    cb_cart_num_1d bigint,
    cb_buy_item_90d bigint,
    cb_buy_item_60d bigint,
    cb_buy_item_30d bigint,
    cb_buy_item_15d bigint,
    cb_buy_item_7d bigint,
    cb_buy_item_3d bigint,
    cb_buy_item_1d bigint,
    cb_buy_num_90d bigint,
    cb_buy_num_60d bigint,
    cb_buy_num_30d bigint,
    cb_buy_num_15d bigint,
    cb_buy_num_7d bigint,
    cb_buy_num_3d bigint,
    cb_buy_num_1d bigint,

    c2_click_item_90d bigint,
    c2_click_item_60d bigint,
    c2_click_item_30d bigint,
    c2_click_item_15d bigint,
    c2_click_item_7d bigint,
    c2_click_item_3d bigint,
    c2_click_item_1d bigint,
    c2_click_num_90d bigint,
    c2_click_num_60d bigint,
    c2_click_num_30d bigint,
    c2_click_num_15d bigint,
    c2_click_num_7d bigint,
    c2_click_num_3d bigint,
    c2_click_num_1d bigint,
    c2_click_day_90d bigint,
    c2_click_day_60d bigint,
    c2_click_day_30d bigint,
    c2_click_day_15d bigint,
    c2_click_day_7d bigint,
    c2_click_day_3d bigint,
    c2_click_day_1d bigint,
    c2_collect_item_90d bigint,
    c2_collect_item_60d bigint,
    c2_collect_item_30d bigint,
    c2_collect_item_15d bigint,
    c2_collect_item_7d bigint,
    c2_collect_item_3d bigint,
    c2_collect_item_1d bigint,
    c2_cart_item_90d bigint,
    c2_cart_item_60d bigint,
    c2_cart_item_30d bigint,
    c2_cart_item_15d bigint,
    c2_cart_item_7d bigint,
    c2_cart_item_3d bigint,
    c2_cart_item_1d bigint,
    c2_cart_num_90d bigint,
    c2_cart_num_60d bigint,
    c2_cart_num_30d bigint,
    c2_cart_num_15d bigint,
    c2_cart_num_7d bigint,
    c2_cart_num_3d bigint,
    c2_cart_num_1d bigint,
    c2_buy_item_90d bigint,
    c2_buy_item_60d bigint,
    c2_buy_item_30d bigint,
    c2_buy_item_15d bigint,
    c2_buy_item_7d bigint,
    c2_buy_item_3d bigint,
    c2_buy_item_1d bigint,
    c2_buy_num_90d bigint,
    c2_buy_num_60d bigint,
    c2_buy_num_30d bigint,
    c2_buy_num_15d bigint,
    c2_buy_num_7d bigint,
    c2_buy_num_3d bigint,
    c2_buy_num_1d bigint
)PARTITIONED by (ds string) LIFECYCLE 60;


insert OVERWRITE TABLE user_pay_sample_feature_join_eval PARTITION (ds=${bizdate})
select t1.user_id, t1.brand_id, t1.label, t1.ds, RAND() as rnd,
    if(t2.item_num_3d is null, 0, t2.item_num_3d),
    if(t2.brand_num_3d is null, 0, t2.brand_num_3d),
    if(t2.seller_num_3d is null, 0, t2.seller_num_3d),
    if(t2.cate1_num_3d is null, 0, t2.cate1_num_3d),
    if(t2.cate2_num_3d is null, 0, t2.cate2_num_3d),
    if(t2.cnt_days_3d is null, 0, t2.cnt_days_3d),
    if(t2.item_num_7d is null, 0, t2.item_num_7d),
    if(t2.brand_num_7d is null, 0, t2.brand_num_7d),
    if(t2.seller_num_7d is null, 0, t2.seller_num_7d),
    if(t2.cate1_num_7d is null, 0, t2.cate1_num_7d),
    if(t2.cate2_num_7d is null, 0, t2.cate2_num_7d),
    if(t2.cnt_days_7d is null, 0, t2.cnt_days_7d),
    if(t2.item_num_15d is null, 0, t2.item_num_15d),
    if(t2.brand_num_15d is null, 0, t2.brand_num_15d),
    if(t2.seller_num_15d is null, 0, t2.seller_num_15d),
    if(t2.cate1_num_15d is null, 0, t2.cate1_num_15d),
    if(t2.cate2_num_15d is null, 0, t2.cate2_num_15d),
    if(t2.cnt_days_15d is null, 0, t2.cnt_days_15d),
    if(t2.item_num_30d is null, 0, t2.item_num_30d),
    if(t2.brand_num_30d is null, 0, t2.brand_num_30d),
    if(t2.seller_num_30d is null, 0, t2.seller_num_30d),
    if(t2.cate1_num_30d is null, 0, t2.cate1_num_30d),
    if(t2.cate2_num_30d is null, 0, t2.cate2_num_30d),
    if(t2.cnt_days_30d is null, 0, t2.cnt_days_30d),
    if(t2.item_num_60d is null, 0, t2.item_num_60d),
    if(t2.brand_num_60d is null, 0, t2.brand_num_60d),
    if(t2.seller_num_60d is null, 0, t2.seller_num_60d),
    if(t2.cate1_num_60d is null, 0, t2.cate1_num_60d),
    if(t2.cate2_num_60d is null, 0, t2.cate2_num_60d),
    if(t2.cnt_days_60d is null, 0, t2.cnt_days_60d),
    if(t2.item_num_90d is null, 0, t2.item_num_90d),
    if(t2.brand_num_90d is null, 0, t2.brand_num_90d),
    if(t2.seller_num_90d is null, 0, t2.seller_num_90d),
    if(t2.cate1_num_90d is null, 0, t2.cate1_num_90d),
    if(t2.cate2_num_90d is null, 0, t2.cate2_num_90d),
    if(t2.cnt_days_90d is null, 0, t2.cnt_days_90d),

    if(t3.item_num_3d is null, 0, t3.item_num_3d),
    if(t3.brand_num_3d is null, 0, t3.brand_num_3d),
    if(t3.seller_num_3d is null, 0, t3.seller_num_3d),
    if(t3.cate1_num_3d is null, 0, t3.cate1_num_3d),
    if(t3.cate2_num_3d is null, 0, t3.cate2_num_3d),
    if(t3.item_num_7d is null, 0, t3.item_num_7d),
    if(t3.brand_num_7d is null, 0, t3.brand_num_7d),
    if(t3.seller_num_7d is null, 0, t3.seller_num_7d),
    if(t3.cate1_num_7d is null, 0, t3.cate1_num_7d),
    if(t3.cate2_num_7d is null, 0, t3.cate2_num_7d),
    if(t3.item_num_15d is null, 0, t3.item_num_15d),
    if(t3.brand_num_15d is null, 0, t3.brand_num_15d),
    if(t3.seller_num_15d is null, 0, t3.seller_num_15d),
    if(t3.cate1_num_15d is null, 0, t3.cate1_num_15d),
    if(t3.cate2_num_15d is null, 0, t3.cate2_num_15d),

    if(t3.item_num_30d is null, 0, t3.item_num_30d),
    if(t3.brand_num_30d is null, 0, t3.brand_num_30d),
    if(t3.seller_num_30d is null, 0, t3.seller_num_30d),
    if(t3.cate1_num_30d is null, 0, t3.cate1_num_30d),
    if(t3.cate2_num_30d is null, 0, t3.cate2_num_30d),

    if(t3.item_num_60d is null, 0, t3.item_num_60d),
    if(t3.brand_num_60d is null, 0, t3.brand_num_60d),
    if(t3.seller_num_60d is null, 0, t3.seller_num_60d),
    if(t3.cate1_num_60d is null, 0, t3.cate1_num_60d),
    if(t3.cate2_num_60d is null, 0, t3.cate2_num_60d),

    if(t3.item_num_90d is null, 0, t3.item_num_90d),
    if(t3.brand_num_90d is null, 0, t3.brand_num_90d),
    if(t3.seller_num_90d is null, 0, t3.seller_num_90d),
    if(t3.cate1_num_90d is null, 0, t3.cate1_num_90d),
    if(t3.cate2_num_90d is null, 0, t3.cate2_num_90d),

    if(t4.item_num_3d is null, 0, t4.item_num_3d),
    if(t4.brand_num_3d is null, 0, t4.brand_num_3d),
    if(t4.seller_num_3d is null, 0, t4.seller_num_3d),
    if(t4.cate1_num_3d is null, 0, t4.cate1_num_3d),
    if(t4.cate2_num_3d is null, 0, t4.cate2_num_3d),
    if(t4.item_num_7d is null, 0, t4.item_num_7d),
    if(t4.brand_num_7d is null, 0, t4.brand_num_7d),
    if(t4.seller_num_7d is null, 0, t4.seller_num_7d),
    if(t4.cate1_num_7d is null, 0, t4.cate1_num_7d),
    if(t4.cate2_num_7d is null, 0, t4.cate2_num_7d),
    if(t4.item_num_15d is null, 0, t4.item_num_15d),
    if(t4.brand_num_15d is null, 0, t4.brand_num_15d),
    if(t4.seller_num_15d is null, 0, t4.seller_num_15d),
    if(t4.cate1_num_15d is null, 0, t4.cate1_num_15d),
    if(t4.cate2_num_15d is null, 0, t4.cate2_num_15d),

    if(t4.item_num_30d is null, 0, t4.item_num_30d),
    if(t4.brand_num_30d is null, 0, t4.brand_num_30d),
    if(t4.seller_num_30d is null, 0, t4.seller_num_30d),
    if(t4.cate1_num_30d is null, 0, t4.cate1_num_30d),
    if(t4.cate2_num_30d is null, 0, t4.cate2_num_30d),

    if(t4.item_num_60d is null, 0, t4.item_num_60d),
    if(t4.brand_num_60d is null, 0, t4.brand_num_60d),
    if(t4.seller_num_60d is null, 0, t4.seller_num_60d),
    if(t4.cate1_num_60d is null, 0, t4.cate1_num_60d),
    if(t4.cate2_num_60d is null, 0, t4.cate2_num_60d),

    if(t4.item_num_90d is null, 0, t4.item_num_90d),
    if(t4.brand_num_90d is null, 0, t4.brand_num_90d),
    if(t4.seller_num_90d is null, 0, t4.seller_num_90d),
    if(t4.cate1_num_90d is null, 0, t4.cate1_num_90d),
    if(t4.cate2_num_90d is null, 0, t4.cate2_num_90d),

    if(t5.item_num_3d is null, 0, t5.item_num_3d),
    if(t5.brand_num_3d is null, 0, t5.brand_num_3d),
    if(t5.seller_num_3d is null, 0, t5.seller_num_3d),
    if(t5.cate1_num_3d is null, 0, t5.cate1_num_3d),
    if(t5.cate2_num_3d is null, 0, t5.cate2_num_3d),
    if(t5.item_num_7d is null, 0, t5.item_num_7d),
    if(t5.brand_num_7d is null, 0, t5.brand_num_7d),
    if(t5.seller_num_7d is null, 0, t5.seller_num_7d),
    if(t5.cate1_num_7d is null, 0, t5.cate1_num_7d),
    if(t5.cate2_num_7d is null, 0, t5.cate2_num_7d),
    if(t5.item_num_15d is null, 0, t5.item_num_15d),
    if(t5.brand_num_15d is null, 0, t5.brand_num_15d),
    if(t5.seller_num_15d is null, 0, t5.seller_num_15d),
    if(t5.cate1_num_15d is null, 0, t5.cate1_num_15d),
    if(t5.cate2_num_15d is null, 0, t5.cate2_num_15d),
    if(t5.item_num_30d is null, 0, t5.item_num_30d),
    if(t5.brand_num_30d is null, 0, t5.brand_num_30d),
    if(t5.seller_num_30d is null, 0, t5.seller_num_30d),
    if(t5.cate1_num_30d is null, 0, t5.cate1_num_30d),
    if(t5.cate2_num_30d is null, 0, t5.cate2_num_30d),
    if(t5.item_num_60d is null, 0, t5.item_num_60d),
    if(t5.brand_num_60d is null, 0, t5.brand_num_60d),
    if(t5.seller_num_60d is null, 0, t5.seller_num_60d),
    if(t5.cate1_num_60d is null, 0, t5.cate1_num_60d),
    if(t5.cate2_num_60d is null, 0, t5.cate2_num_60d),
    if(t5.item_num_90d is null, 0, t5.item_num_90d),
    if(t5.brand_num_90d is null, 0, t5.brand_num_90d),
    if(t5.seller_num_90d is null, 0, t5.seller_num_90d),
    if(t5.cate1_num_90d is null, 0, t5.cate1_num_90d),
    if(t5.cate2_num_90d is null, 0, t5.cate2_num_90d),

    if(t6.click_num is null, 0, t6.click_num),
    if(t6.collect_num is null, 0, t6.collect_num),
    if(t6.cart_num is null, 0, t6.cart_num),
    if(t6.buy_num is null, 0, t6.buy_num),

    if(t7.click_item_90d is null, 0, t7.click_item_90d),
    if(t7.click_item_60d is null, 0, t7.click_item_60d),
    if(t7.click_item_30d is null, 0, t7.click_item_30d),
    if(t7.click_item_15d is null, 0, t7.click_item_15d),
    if(t7.click_item_7d is null, 0, t7.click_item_7d),
    if(t7.click_item_3d is null, 0, t7.click_item_3d),
    if(t7.click_item_1d is null, 0, t7.click_item_1d),
    if(t7.click_num_90d is null, 0, t7.click_num_90d),
    if(t7.click_num_60d is null, 0, t7.click_num_60d),
    if(t7.click_num_30d is null, 0, t7.click_num_30d),
    if(t7.click_num_15d is null, 0, t7.click_num_15d),
    if(t7.click_num_7d is null, 0, t7.click_num_7d),
    if(t7.click_num_3d is null, 0, t7.click_num_3d),
    if(t7.click_num_1d is null, 0, t7.click_num_1d),
    if(t7.click_day_90d is null, 0, t7.click_day_90d),
    if(t7.click_day_60d is null, 0, t7.click_day_60d),
    if(t7.click_day_30d is null, 0, t7.click_day_30d),
    if(t7.click_day_15d is null, 0, t7.click_day_15d),
    if(t7.click_day_7d is null, 0, t7.click_day_7d),
    if(t7.click_day_3d is null, 0, t7.click_day_3d),
    if(t7.click_day_1d is null, 0, t7.click_day_1d),
    if(t7.collect_item_90d is null, 0, t7.collect_item_90d),
    if(t7.collect_item_60d is null, 0, t7.collect_item_60d),
    if(t7.collect_item_30d is null, 0, t7.collect_item_30d),
    if(t7.collect_item_15d is null, 0, t7.collect_item_15d),
    if(t7.collect_item_7d is null, 0, t7.collect_item_7d),
    if(t7.collect_item_3d is null, 0, t7.collect_item_3d),
    if(t7.collect_item_1d is null, 0, t7.collect_item_1d),
    if(t7.cart_item_90d is null, 0, t7.cart_item_90d),
    if(t7.cart_item_60d is null, 0, t7.cart_item_60d),
    if(t7.cart_item_30d is null, 0, t7.cart_item_30d),
    if(t7.cart_item_15d is null, 0, t7.cart_item_15d),
    if(t7.cart_item_7d is null, 0, t7.cart_item_7d),
    if(t7.cart_item_3d is null, 0, t7.cart_item_3d),
    if(t7.cart_item_1d is null, 0, t7.cart_item_1d),
    if(t7.cart_num_90d is null, 0, t7.cart_num_90d),
    if(t7.cart_num_60d is null, 0, t7.cart_num_60d),
    if(t7.cart_num_30d is null, 0, t7.cart_num_30d),
    if(t7.cart_num_15d is null, 0, t7.cart_num_15d),
    if(t7.cart_num_7d is null, 0, t7.cart_num_7d),
    if(t7.cart_num_3d is null, 0, t7.cart_num_3d),
    if(t7.cart_num_1d is null, 0, t7.cart_num_1d),
    if(t7.buy_item_90d is null, 0, t7.buy_item_90d),
    if(t7.buy_item_60d is null, 0, t7.buy_item_60d),
    if(t7.buy_item_30d is null, 0, t7.buy_item_30d),
    if(t7.buy_item_15d is null, 0, t7.buy_item_15d),
    if(t7.buy_item_7d is null, 0, t7.buy_item_7d),
    if(t7.buy_item_3d is null, 0, t7.buy_item_3d),
    if(t7.buy_item_1d is null, 0, t7.buy_item_1d),
    if(t7.buy_num_90d is null, 0, t7.buy_num_90d),
    if(t7.buy_num_60d is null, 0, t7.buy_num_60d),
    if(t7.buy_num_30d is null, 0, t7.buy_num_30d),
    if(t7.buy_num_15d is null, 0, t7.buy_num_15d),
    if(t7.buy_num_7d is null, 0, t7.buy_num_7d),
    if(t7.buy_num_3d is null, 0, t7.buy_num_3d),
    if(t7.buy_num_1d is null, 0, t7.buy_num_1d),

    if(t8.click_item_90d is null, 0, t8.click_item_90d),
    if(t8.click_item_60d is null, 0, t8.click_item_60d),
    if(t8.click_item_30d is null, 0, t8.click_item_30d),
    if(t8.click_item_15d is null, 0, t8.click_item_15d),
    if(t8.click_item_7d is null, 0, t8.click_item_7d),
    if(t8.click_item_3d is null, 0, t8.click_item_3d),
    if(t8.click_item_1d is null, 0, t8.click_item_1d),
    if(t8.click_num_90d is null, 0, t8.click_num_90d),
    if(t8.click_num_60d is null, 0, t8.click_num_60d),
    if(t8.click_num_30d is null, 0, t8.click_num_30d),
    if(t8.click_num_15d is null, 0, t8.click_num_15d),
    if(t8.click_num_7d is null, 0, t8.click_num_7d),
    if(t8.click_num_3d is null, 0, t8.click_num_3d),
    if(t8.click_num_1d is null, 0, t8.click_num_1d),
    if(t8.click_day_90d is null, 0, t8.click_day_90d),
    if(t8.click_day_60d is null, 0, t8.click_day_60d),
    if(t8.click_day_30d is null, 0, t8.click_day_30d),
    if(t8.click_day_15d is null, 0, t8.click_day_15d),
    if(t8.click_day_7d is null, 0, t8.click_day_7d),
    if(t8.click_day_3d is null, 0, t8.click_day_3d),
    if(t8.click_day_1d is null, 0, t8.click_day_1d),
    if(t8.collect_item_90d is null, 0, t8.collect_item_90d),
    if(t8.collect_item_60d is null, 0, t8.collect_item_60d),
    if(t8.collect_item_30d is null, 0, t8.collect_item_30d),
    if(t8.collect_item_15d is null, 0, t8.collect_item_15d),
    if(t8.collect_item_7d is null, 0, t8.collect_item_7d),
    if(t8.collect_item_3d is null, 0, t8.collect_item_3d),
    if(t8.collect_item_1d is null, 0, t8.collect_item_1d),
    if(t8.cart_item_90d is null, 0, t8.cart_item_90d),
    if(t8.cart_item_60d is null, 0, t8.cart_item_60d),
    if(t8.cart_item_30d is null, 0, t8.cart_item_30d),
    if(t8.cart_item_15d is null, 0, t8.cart_item_15d),
    if(t8.cart_item_7d is null, 0, t8.cart_item_7d),
    if(t8.cart_item_3d is null, 0, t8.cart_item_3d),
    if(t8.cart_item_1d is null, 0, t8.cart_item_1d),
    if(t8.cart_num_90d is null, 0, t8.cart_num_90d),
    if(t8.cart_num_60d is null, 0, t8.cart_num_60d),
    if(t8.cart_num_30d is null, 0, t8.cart_num_30d),
    if(t8.cart_num_15d is null, 0, t8.cart_num_15d),
    if(t8.cart_num_7d is null, 0, t8.cart_num_7d),
    if(t8.cart_num_3d is null, 0, t8.cart_num_3d),
    if(t8.cart_num_1d is null, 0, t8.cart_num_1d),
    if(t8.buy_item_90d is null, 0, t8.buy_item_90d),
    if(t8.buy_item_60d is null, 0, t8.buy_item_60d),
    if(t8.buy_item_30d is null, 0, t8.buy_item_30d),
    if(t8.buy_item_15d is null, 0, t8.buy_item_15d),
    if(t8.buy_item_7d is null, 0, t8.buy_item_7d),
    if(t8.buy_item_3d is null, 0, t8.buy_item_3d),
    if(t8.buy_item_1d is null, 0, t8.buy_item_1d),
    if(t8.buy_num_90d is null, 0, t8.buy_num_90d),
    if(t8.buy_num_60d is null, 0, t8.buy_num_60d),
    if(t8.buy_num_30d is null, 0, t8.buy_num_30d),
    if(t8.buy_num_15d is null, 0, t8.buy_num_15d),
    if(t8.buy_num_7d is null, 0, t8.buy_num_7d),
    if(t8.buy_num_3d is null, 0, t8.buy_num_3d),
    if(t8.buy_num_1d is null, 0, t8.buy_num_1d)

FROM (
    SELECT user_id, brand_id, label, ds
    FROM user_pay_sample_eval 
    where ds=${bizdate}
) t1 left join(
    SELECT *
    FROM user_click_beh_feature_ads 
    where ds=${bizdate}
) t2 on t1.user_id=t2.user_id left join(
    SELECT *
    FROM user_collect_beh_feature_ads 
    where ds=${bizdate}
) t3 on t1.user_id=t3.user_id left join(
    SELECT *
    FROM user_cart_beh_feature_ads 
    where ds=${bizdate}
) t4 on t1.user_id=t4.user_id left join(
    SELECT *
    FROM user_buy_beh_feature_ads 
    where ds=${bizdate}
) t5 on t1.user_id=t5.user_id left join(
    SELECT *
    FROM brand_stat_feature_ads 
    where ds=${bizdate} and brand_id in ('b47686','b56508','b62063','b78739')
) t6 on t1.brand_id=t6.brand_id left join(
    SELECT *
    FROM user_brand_cross_beh_feature_ads 
    where ds=${bizdate} and brand_id in ('b47686', 'b56508', 'b62063', 'b78739')
) t7 on t1.user_id=t7.user_id and t1.brand_id=t7.brand_id and t1.ds=t7.ds left join(
    SELECT *
    FROM user_brand_cate2_cross_beh_feature_ads 
    where ds=${bizdate} and brand_id in ('b47686', 'b56508', 'b62063', 'b78739')
) t8 on t1.user_id=t8.user_id and t1.brand_id=t8.brand_id  and t1.ds=t8.ds
WHERE (t2.cnt_days_90d is not null or t2.cate1_num_90d is not null or t3.item_num_90d is not null
    or t4.item_num_90d is not null or t5.item_num_90d is not null 
    or t7.click_item_90d is not null or t8.click_item_90d is not null)
;



show partitions user_pay_sample_eval;

SELECT count(*)
FROM user_pay_sample_feature_join_eval
WHERE ds='20130923' and brand_id = 'b78739';
