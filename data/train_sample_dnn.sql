--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-11 20:48:00
--********************************************************************--

create table if not exists user_pay_sample_feature_join_dnn(
    user_id string,
    brand_id string,
    label bigint,
    target_brand_id string,
    click_brand_seq string,
    collect_brand_seq string,
    pay_brand_seq string,
    click_cate_seq string,
    collect_cate_seq string,
    pay_cate_seq string,
    user_click_feature string,
    user_collect_feature string,
    user_cart_feature string,
    user_pay_feature string,
    brand_stat_feature string,
    user_cate2_cross_feature string,
    user_brand_cross_feature string
)PARTITIONED BY (ds STRING ) LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_join_dnn partition (ds=${bizdate})
select t1.user_id, t1.brand_id, t1.label, concat('b_',t1.brand_id)
    ,if(t2.brand_id_seq is null, 'clickb_seq_null', t2.brand_id_seq)
    ,if(t3.brand_id_seq is null, 'collectb_seq_null', t3.brand_id_seq)
    ,if(t4.brand_id_seq is null, 'payb_seq_null', t4.brand_id_seq)
    ,if(t5.cate_seq is null, 'clickc_seq_null', t5.cate_seq)
    ,if(t6.cate_seq is null, 'collectc_seq_null', t6.cate_seq)
    ,if(t7.cate_seq is null, 'payc_seq_null', t7.cate_seq)
    ,if(t8.user_id is null, 'user_click_null', concat(
        concat('uclick_item_num_3d','_',if(t8.item_num_3d is null, 'null', cast(log(2,t8.item_num_3d+1) as bigint))),',',
        concat('uclick_brand_num_3d','_',if(t8.brand_num_3d is null, 'null', cast(log(2,t8.brand_num_3d+1) as bigint))),',',
        concat('uclick_seller_num_3d','_',if(t8.seller_num_3d is null, 'null', cast(log(2,t8.seller_num_3d+1) as bigint))),',',
        concat('uclick_cate1_num_3d','_',if(t8.cate1_num_3d is null, 'null', cast(log(2,t8.cate1_num_3d+1) as bigint))),',',
        concat('uclick_cate2_num_3d','_',if(t8.cate2_num_3d is null, 'null', cast(log(2,t8.cate2_num_3d+1) as bigint))),',',
        concat('uclick_cnt_days_3d','_',if(t8.cnt_days_3d is null, 'null', cast(log(2,t8.cnt_days_3d+1) as bigint))),',',
        concat('uclick_item_num_7d','_',if(t8.item_num_7d is null, 'null', cast(log(2,t8.item_num_7d+1) as bigint))),',',
        concat('uclick_brand_num_7d','_',if(t8.brand_num_7d is null, 'null', cast(log(2,t8.brand_num_7d+1) as bigint))),',',
        concat('uclick_seller_num_7d','_',if(t8.seller_num_7d is null, 'null', cast(log(2,t8.seller_num_7d+1) as bigint))),',',
        concat('uclick_cate1_num_7d','_',if(t8.cate1_num_7d is null, 'null', cast(log(2,t8.cate1_num_7d+1) as bigint))),',',
        concat('uclick_cate2_num_7d','_',if(t8.cate2_num_7d is null, 'null', cast(log(2,t8.cate2_num_7d+1) as bigint))),',',
        concat('uclick_cnt_days_7d','_',if(t8.cnt_days_7d is null, 'null', cast(log(2,t8.cnt_days_7d+1) as bigint))),',',
        concat('uclick_item_num_15d','_',if(t8.item_num_15d is null, 'null', cast(log(2,t8.item_num_15d+1) as bigint))),',',
        concat('uclick_brand_num_15d','_',if(t8.brand_num_15d is null, 'null', cast(log(2,t8.brand_num_15d+1) as bigint))),',',
        concat('uclick_seller_num_15d','_',if(t8.seller_num_15d is null, 'null', cast(log(2,t8.seller_num_15d+1) as bigint))),',',
        concat('uclick_cate1_num_15d','_',if(t8.cate1_num_15d is null, 'null', cast(log(2,t8.cate1_num_15d+1) as bigint))),',',
        concat('uclick_cate2_num_15d','_',if(t8.cate2_num_15d is null, 'null', cast(log(2,t8.cate2_num_15d+1) as bigint))),',',
        concat('uclick_cnt_days_15d','_',if(t8.cnt_days_15d is null, 'null', cast(log(2,t8.cnt_days_15d+1) as bigint))),',',
        concat('uclick_item_num_30d','_',if(t8.item_num_30d is null, 'null', cast(log(2,t8.item_num_30d+1) as bigint))),',',
        concat('uclick_brand_num_30d','_',if(t8.brand_num_30d is null, 'null', cast(log(2,t8.brand_num_30d+1) as bigint))),',',
        concat('uclick_seller_num_30d','_',if(t8.seller_num_30d is null, 'null', cast(log(2,t8.seller_num_30d+1) as bigint))),',',
        concat('uclick_cate1_num_30d','_',if(t8.cate1_num_30d is null, 'null', cast(log(2,t8.cate1_num_30d+1) as bigint))),',',
        concat('uclick_cate2_num_30d','_',if(t8.cate2_num_30d is null, 'null', cast(log(2,t8.cate2_num_30d+1) as bigint))),',',
        concat('uclick_cnt_days_30d','_',if(t8.cnt_days_30d is null, 'null', cast(log(2,t8.cnt_days_30d+1) as bigint))),',',
        concat('uclick_item_num_60d','_',if(t8.item_num_60d is null, 'null', cast(log(2,t8.item_num_60d+1) as bigint))),',',
        concat('uclick_brand_num_60d','_',if(t8.brand_num_60d is null, 'null', cast(log(2,t8.brand_num_60d+1) as bigint))),',',
        concat('uclick_seller_num_60d','_',if(t8.seller_num_60d is null, 'null', cast(log(2,t8.seller_num_60d+1) as bigint))),',',
        concat('uclick_cate1_num_60d','_',if(t8.cate1_num_60d is null, 'null', cast(log(2,t8.cate1_num_60d+1) as bigint))),',',
        concat('uclick_cate2_num_60d','_',if(t8.cate2_num_60d is null, 'null', cast(log(2,t8.cate2_num_60d+1) as bigint))),',',
        concat('uclick_cnt_days_60d','_',if(t8.cnt_days_60d is null, 'null', cast(log(2,t8.cnt_days_60d+1) as bigint))),',',
        concat('uclick_item_num_90d','_',if(t8.item_num_90d is null, 'null', cast(log(2,t8.item_num_90d+1) as bigint))),',',
        concat('uclick_brand_num_90d','_',if(t8.brand_num_90d is null, 'null', cast(log(2,t8.brand_num_90d+1) as bigint))),',',
        concat('uclick_seller_num_90d','_',if(t8.seller_num_90d is null, 'null', cast(log(2,t8.seller_num_90d+1) as bigint))),',',
        concat('uclick_cate1_num_90d','_',if(t8.cate1_num_90d is null, 'null', cast(log(2,t8.cate1_num_90d+1) as bigint))),',',
        concat('uclick_cate2_num_90d','_',if(t8.cate2_num_90d is null, 'null', cast(log(2,t8.cate2_num_90d+1) as bigint))),',',
        concat('uclick_cnt_days_90d','_',if(t8.cnt_days_90d is null, 'null', cast(log(2,t8.cnt_days_90d+1) as bigint)))
    )) as user_click_feature
    ,if(t9.user_id is null, 'user_collect_null',concat(
        concat('ucollect_item_num_3d','_',if(t9.item_num_3d is null, 'null', cast(log(2,t9.item_num_3d+1) as bigint))),',',
        concat('ucollect_brand_num_3d','_',if(t9.brand_num_3d is null, 'null', cast(log(2,t9.brand_num_3d+1) as bigint))),',',
        concat('ucollect_seller_num_3d','_',if(t9.seller_num_3d is null, 'null', cast(log(2,t9.seller_num_3d+1) as bigint))),',',
        concat('ucollect_cate1_num_3d','_',if(t9.cate1_num_3d is null, 'null', cast(log(2,t9.cate1_num_3d+1) as bigint))),',',
        concat('ucollect_cate2_num_3d','_',if(t9.cate2_num_3d is null, 'null', cast(log(2,t9.cate2_num_3d+1) as bigint))),',',
        concat('ucollect_item_num_7d','_',if(t9.item_num_7d is null, 'null', cast(log(2,t9.item_num_7d+1) as bigint))),',',
        concat('ucollect_brand_num_7d','_',if(t9.brand_num_7d is null, 'null', cast(log(2,t9.brand_num_7d+1) as bigint))),',',
        concat('ucollect_seller_num_7d','_',if(t9.seller_num_7d is null, 'null', cast(log(2,t9.seller_num_7d+1) as bigint))),',',
        concat('ucollect_cate1_num_7d','_',if(t9.cate1_num_7d is null, 'null', cast(log(2,t9.cate1_num_7d+1) as bigint))),',',
        concat('ucollect_cate2_num_7d','_',if(t9.cate2_num_7d is null, 'null', cast(log(2,t9.cate2_num_7d+1) as bigint))),',',
        concat('ucollect_item_num_15d','_',if(t9.item_num_15d is null, 'null', cast(log(2,t9.item_num_15d+1) as bigint))),',',
        concat('ucollect_brand_num_15d','_',if(t9.brand_num_15d is null, 'null', cast(log(2,t9.brand_num_15d+1) as bigint))),',',
        concat('ucollect_seller_num_15d','_',if(t9.seller_num_15d is null, 'null', cast(log(2,t9.seller_num_15d+1) as bigint))),',',
        concat('ucollect_cate1_num_15d','_',if(t9.cate1_num_15d is null, 'null', cast(log(2,t9.cate1_num_15d+1) as bigint))),',',
        concat('ucollect_cate2_num_15d','_',if(t9.cate2_num_15d is null, 'null', cast(log(2,t9.cate2_num_15d+1) as bigint))),',',
        concat('ucollect_item_num_30d','_',if(t9.item_num_30d is null, 'null', cast(log(2,t9.item_num_30d+1) as bigint))),',',
        concat('ucollect_brand_num_30d','_',if(t9.brand_num_30d is null, 'null', cast(log(2,t9.brand_num_30d+1) as bigint))),',',
        concat('ucollect_seller_num_30d','_',if(t9.seller_num_30d is null, 'null', cast(log(2,t9.seller_num_30d+1) as bigint))),',',
        concat('ucollect_cate1_num_30d','_',if(t9.cate1_num_30d is null, 'null', cast(log(2,t9.cate1_num_30d+1) as bigint))),',',
        concat('ucollect_cate2_num_30d','_',if(t9.cate2_num_30d is null, 'null', cast(log(2,t9.cate2_num_30d+1) as bigint))),',',
        concat('ucollect_item_num_60d','_',if(t9.item_num_60d is null, 'null', cast(log(2,t9.item_num_60d+1) as bigint))),',',
        concat('ucollect_brand_num_60d','_',if(t9.brand_num_60d is null, 'null', cast(log(2,t9.brand_num_60d+1) as bigint))),',',
        concat('ucollect_seller_num_60d','_',if(t9.seller_num_60d is null, 'null', cast(log(2,t9.seller_num_60d+1) as bigint))),',',
        concat('ucollect_cate1_num_60d','_',if(t9.cate1_num_60d is null, 'null', cast(log(2,t9.cate1_num_60d+1) as bigint))),',',
        concat('ucollect_cate2_num_60d','_',if(t9.cate2_num_60d is null, 'null', cast(log(2,t9.cate2_num_60d+1) as bigint))),',',
        concat('ucollect_item_num_90d','_',if(t9.item_num_90d is null, 'null', cast(log(2,t9.item_num_90d+1) as bigint))),',',
        concat('ucollect_brand_num_90d','_',if(t9.brand_num_90d is null, 'null', cast(log(2,t9.brand_num_90d+1) as bigint))),',',
        concat('ucollect_seller_num_90d','_',if(t9.seller_num_90d is null, 'null', cast(log(2,t9.seller_num_90d+1) as bigint))),',',
        concat('ucollect_cate1_num_90d','_',if(t9.cate1_num_90d is null, 'null', cast(log(2,t9.cate1_num_90d+1) as bigint))),',',
        concat('ucollect_cate2_num_90d','_',if(t9.cate2_num_90d is null, 'null', cast(log(2,t9.cate2_num_90d+1) as bigint)))
    )) as user_collect_feature
    ,if(t10.user_id is null, 'user_cart_null',concat(
        concat('ucart_item_num_3d','_',if(t10.item_num_3d is null, 'null', cast(log(2,t10.item_num_3d+1) as bigint))),',',
        concat('ucart_brand_num_3d','_',if(t10.brand_num_3d is null, 'null', cast(log(2,t10.brand_num_3d+1) as bigint))),',',
        concat('ucart_seller_num_3d','_',if(t10.seller_num_3d is null, 'null', cast(log(2,t10.seller_num_3d+1) as bigint))),',',
        concat('ucart_cate1_num_3d','_',if(t10.cate1_num_3d is null, 'null', cast(log(2,t10.cate1_num_3d+1) as bigint))),',',
        concat('ucart_cate2_num_3d','_',if(t10.cate2_num_3d is null, 'null', cast(log(2,t10.cate2_num_3d+1) as bigint))),',',
        concat('ucart_item_num_7d','_',if(t10.item_num_7d is null, 'null', cast(log(2,t10.item_num_7d+1) as bigint))),',',
        concat('ucart_brand_num_7d','_',if(t10.brand_num_7d is null, 'null', cast(log(2,t10.brand_num_7d+1) as bigint))),',',
        concat('ucart_seller_num_7d','_',if(t10.seller_num_7d is null, 'null', cast(log(2,t10.seller_num_7d+1) as bigint))),',',
        concat('ucart_cate1_num_7d','_',if(t10.cate1_num_7d is null, 'null', cast(log(2,t10.cate1_num_7d+1) as bigint))),',',
        concat('ucart_cate2_num_7d','_',if(t10.cate2_num_7d is null, 'null', cast(log(2,t10.cate2_num_7d+1) as bigint))),',',
        concat('ucart_item_num_15d','_',if(t10.item_num_15d is null, 'null', cast(log(2,t10.item_num_15d+1) as bigint))),',',
        concat('ucart_brand_num_15d','_',if(t10.brand_num_15d is null, 'null', cast(log(2,t10.brand_num_15d+1) as bigint))),',',
        concat('ucart_seller_num_15d','_',if(t10.seller_num_15d is null, 'null', cast(log(2,t10.seller_num_15d+1) as bigint))),',',
        concat('ucart_cate1_num_15d','_',if(t10.cate1_num_15d is null, 'null', cast(log(2,t10.cate1_num_15d+1) as bigint))),',',
        concat('ucart_cate2_num_15d','_',if(t10.cate2_num_15d is null, 'null', cast(log(2,t10.cate2_num_15d+1) as bigint))),',',
        concat('ucart_item_num_30d','_',if(t10.item_num_30d is null, 'null', cast(log(2,t10.item_num_30d+1) as bigint))),',',
        concat('ucart_brand_num_30d','_',if(t10.brand_num_30d is null, 'null', cast(log(2,t10.brand_num_30d+1) as bigint))),',',
        concat('ucart_seller_num_30d','_',if(t10.seller_num_30d is null, 'null', cast(log(2,t10.seller_num_30d+1) as bigint))),',',
        concat('ucart_cate1_num_30d','_',if(t10.cate1_num_30d is null, 'null', cast(log(2,t10.cate1_num_30d+1) as bigint))),',',
        concat('ucart_cate2_num_30d','_',if(t10.cate2_num_30d is null, 'null', cast(log(2,t10.cate2_num_30d+1) as bigint))),',',
        concat('ucart_item_num_60d','_',if(t10.item_num_60d is null, 'null', cast(log(2,t10.item_num_60d+1) as bigint))),',',
        concat('ucart_brand_num_60d','_',if(t10.brand_num_60d is null, 'null', cast(log(2,t10.brand_num_60d+1) as bigint))),',',
        concat('ucart_seller_num_60d','_',if(t10.seller_num_60d is null, 'null', cast(log(2,t10.seller_num_60d+1) as bigint))),',',
        concat('ucart_cate1_num_60d','_',if(t10.cate1_num_60d is null, 'null', cast(log(2,t10.cate1_num_60d+1) as bigint))),',',
        concat('ucart_cate2_num_60d','_',if(t10.cate2_num_60d is null, 'null', cast(log(2,t10.cate2_num_60d+1) as bigint))),',',
        concat('ucart_item_num_90d','_',if(t10.item_num_90d is null, 'null', cast(log(2,t10.item_num_90d+1) as bigint))),',',
        concat('ucart_brand_num_90d','_',if(t10.brand_num_90d is null, 'null', cast(log(2,t10.brand_num_90d+1) as bigint))),',',
        concat('ucart_seller_num_90d','_',if(t10.seller_num_90d is null, 'null', cast(log(2,t10.seller_num_90d+1) as bigint))),',',
        concat('ucart_cate1_num_90d','_',if(t10.cate1_num_90d is null, 'null', cast(log(2,t10.cate1_num_90d+1) as bigint))),',',
        concat('ucart_cate2_num_90d','_',if(t10.cate2_num_90d is null, 'null', cast(log(2,t10.cate2_num_90d+1) as bigint)))
    )) as user_cart_feature
    ,if(t11.user_id is null, 'user_pay_null',concat(
        concat('ubuy_item_num_3d','_',if(t11.item_num_3d is null, 'null', cast(log(2,t11.item_num_3d+1) as bigint))),',',
        concat('upay_brand_num_3d','_',if(t11.brand_num_3d is null, 'null', cast(log(2,t11.brand_num_3d+1) as bigint))),',',
        concat('upay_seller_num_3d','_',if(t11.seller_num_3d is null, 'null', cast(log(2,t11.seller_num_3d+1) as bigint))),',',
        concat('upay_cate1_num_3d','_',if(t11.cate1_num_3d is null, 'null', cast(log(2,t11.cate1_num_3d+1) as bigint))),',',
        concat('upay_cate2_num_3d','_',if(t11.cate2_num_3d is null, 'null', cast(log(2,t11.cate2_num_3d+1) as bigint))),',',
        concat('ubuy_item_num_7d','_',if(t11.item_num_7d is null, 'null', cast(log(2,t11.item_num_7d+1) as bigint))),',',
        concat('upay_brand_num_7d','_',if(t11.brand_num_7d is null, 'null', cast(log(2,t11.brand_num_7d+1) as bigint))),',',
        concat('upay_seller_num_7d','_',if(t11.seller_num_7d is null, 'null', cast(log(2,t11.seller_num_7d+1) as bigint))),',',
        concat('upay_cate1_num_7d','_',if(t11.cate1_num_7d is null, 'null', cast(log(2,t11.cate1_num_7d+1) as bigint))),',',
        concat('upay_cate2_num_7d','_',if(t11.cate2_num_7d is null, 'null', cast(log(2,t11.cate2_num_7d+1) as bigint))),',',
        concat('ubuy_item_num_15d','_',if(t11.item_num_15d is null, 'null', cast(log(2,t11.item_num_15d+1) as bigint))),',',
        concat('upay_brand_num_15d','_',if(t11.brand_num_15d is null, 'null', cast(log(2,t11.brand_num_15d+1) as bigint))),',',
        concat('upay_seller_num_15d','_',if(t11.seller_num_15d is null, 'null', cast(log(2,t11.seller_num_15d+1) as bigint))),',',
        concat('upay_cate1_num_15d','_',if(t11.cate1_num_15d is null, 'null', cast(log(2,t11.cate1_num_15d+1) as bigint))),',',
        concat('upay_cate2_num_15d','_',if(t11.cate2_num_15d is null, 'null', cast(log(2,t11.cate2_num_15d+1) as bigint))),',',
        concat('ubuy_item_num_30d','_',if(t11.item_num_30d is null, 'null', cast(log(2,t11.item_num_30d+1) as bigint))),',',
        concat('upay_brand_num_30d','_',if(t11.brand_num_30d is null, 'null', cast(log(2,t11.brand_num_30d+1) as bigint))),',',
        concat('upay_seller_num_30d','_',if(t11.seller_num_30d is null, 'null', cast(log(2,t11.seller_num_30d+1) as bigint))),',',
        concat('upay_cate1_num_30d','_',if(t11.cate1_num_30d is null, 'null', cast(log(2,t11.cate1_num_30d+1) as bigint))),',',
        concat('upay_cate2_num_30d','_',if(t11.cate2_num_30d is null, 'null', cast(log(2,t11.cate2_num_30d+1) as bigint))),',',
        concat('ubuy_item_num_60d','_',if(t11.item_num_60d is null, 'null', cast(log(2,t11.item_num_60d+1) as bigint))),',',
        concat('upay_brand_num_60d','_',if(t11.brand_num_60d is null, 'null', cast(log(2,t11.brand_num_60d+1) as bigint))),',',
        concat('upay_seller_num_60d','_',if(t11.seller_num_60d is null, 'null', cast(log(2,t11.seller_num_60d+1) as bigint))),',',
        concat('upay_cate1_num_60d','_',if(t11.cate1_num_60d is null, 'null', cast(log(2,t11.cate1_num_60d+1) as bigint))),',',
        concat('upay_cate2_num_60d','_',if(t11.cate2_num_60d is null, 'null', cast(log(2,t11.cate2_num_60d+1) as bigint))),',',
        concat('ubuy_item_num_90d','_',if(t11.item_num_90d is null, 'null', cast(log(2,t11.item_num_90d+1) as bigint))),',',
        concat('upay_brand_num_90d','_',if(t11.brand_num_90d is null, 'null', cast(log(2,t11.brand_num_90d+1) as bigint))),',',
        concat('upay_seller_num_90d','_',if(t11.seller_num_90d is null, 'null', cast(log(2,t11.seller_num_90d+1) as bigint))),',',
        concat('upay_cate1_num_90d','_',if(t11.cate1_num_90d is null, 'null', cast(log(2,t11.cate1_num_90d+1) as bigint))),',',
        concat('upay_cate2_num_90d','_',if(t11.cate2_num_90d is null, 'null', cast(log(2,t11.cate2_num_90d+1) as bigint)))
            )) as user_pay_feature
    ,if(t12.brand_id is null, 'brand_stat_null',concat(
        concat('b_click_num','_',if(t12.click_num is null, 'null', cast(log(2,t12.click_num+1) as bigint))),',',
        concat('b_collect_num','_',if(t12.collect_num is null, 'null', cast(log(2,t12.collect_num+1) as bigint))),',',
        concat('b_cart_num','_',if(t12.cart_num is null, 'null', cast(log(2,t12.cart_num+1) as bigint))),',',
        concat('b_buy_num','_',if(t12.buy_num is null, 'null', cast(log(2,t12.buy_num+1) as bigint)))
    )) as brand_stat_feature
    ,if(t13.user_id is null, 'uc2_null',concat(
        concat('uc2_click_item_90d','_',if(t13.click_item_90d is null, 'null', cast(log(2,t13.click_item_90d+1) as bigint))),',',
        concat('uc2_click_item_60d','_',if(t13.click_item_60d is null, 'null', cast(log(2,t13.click_item_60d+1) as bigint))),',',
        concat('uc2_click_item_30d','_',if(t13.click_item_30d is null, 'null', cast(log(2,t13.click_item_30d+1) as bigint))),',',
        concat('uc2_click_item_15d','_',if(t13.click_item_15d is null, 'null', cast(log(2,t13.click_item_15d+1) as bigint))),',',
        concat('uc2_click_item_7d','_',if(t13.click_item_7d is null, 'null', cast(log(2,t13.click_item_7d+1) as bigint))),',',
        concat('uc2_click_item_3d','_',if(t13.click_item_3d is null, 'null', cast(log(2,t13.click_item_3d+1) as bigint))),',',
        concat('uc2_click_item_1d','_',if(t13.click_item_1d is null, 'null', cast(log(2,t13.click_item_1d+1) as bigint))),',',
        concat('uc2_click_num_90d','_',if(t13.click_num_90d is null, 'null', cast(log(2,t13.click_num_90d+1) as bigint))),',',
        concat('uc2_click_num_60d','_',if(t13.click_num_60d is null, 'null', cast(log(2,t13.click_num_60d+1) as bigint))),',',
        concat('uc2_click_num_30d','_',if(t13.click_num_30d is null, 'null', cast(log(2,t13.click_num_30d+1) as bigint))),',',
        concat('uc2_click_num_15d','_',if(t13.click_num_15d is null, 'null', cast(log(2,t13.click_num_15d+1) as bigint))),',',
        concat('uc2_click_num_7d','_',if(t13.click_num_7d is null, 'null', cast(log(2,t13.click_num_7d+1) as bigint))),',',
        concat('uc2_click_num_3d','_',if(t13.click_num_3d is null, 'null', cast(log(2,t13.click_num_3d+1) as bigint))),',',
        concat('uc2_click_num_1d','_',if(t13.click_num_1d is null, 'null', cast(log(2,t13.click_num_1d+1) as bigint))),',',
        concat('uc2_click_day_90d','_',if(t13.click_day_90d is null, 'null', cast(log(2,t13.click_day_90d+1) as bigint))),',',
        concat('uc2_click_day_60d','_',if(t13.click_day_60d is null, 'null', cast(log(2,t13.click_day_60d+1) as bigint))),',',
        concat('uc2_click_day_30d','_',if(t13.click_day_30d is null, 'null', cast(log(2,t13.click_day_30d+1) as bigint))),',',
        concat('uc2_click_day_15d','_',if(t13.click_day_15d is null, 'null', cast(log(2,t13.click_day_15d+1) as bigint))),',',
        concat('uc2_click_day_7d','_',if(t13.click_day_7d is null, 'null', cast(log(2,t13.click_day_7d+1) as bigint))),',',
        concat('uc2_click_day_3d','_',if(t13.click_day_3d is null, 'null', cast(log(2,t13.click_day_3d+1) as bigint))),',',
        concat('uc2_click_day_1d','_',if(t13.click_day_1d is null, 'null', cast(log(2,t13.click_day_1d+1) as bigint))),',',
        concat('uc2_collect_item_90d','_',if(t13.collect_item_90d is null, 'null', cast(log(2,t13.collect_item_90d+1) as bigint))),',',
        concat('uc2_collect_item_60d','_',if(t13.collect_item_60d is null, 'null', cast(log(2,t13.collect_item_60d+1) as bigint))),',',
        concat('uc2_collect_item_30d','_',if(t13.collect_item_30d is null, 'null', cast(log(2,t13.collect_item_30d+1) as bigint))),',',
        concat('uc2_collect_item_15d','_',if(t13.collect_item_15d is null, 'null', cast(log(2,t13.collect_item_15d+1) as bigint))),',',
        concat('uc2_collect_item_7d','_',if(t13.collect_item_7d is null, 'null', cast(log(2,t13.collect_item_7d+1) as bigint))),',',
        concat('uc2_collect_item_3d','_',if(t13.collect_item_3d is null, 'null', cast(log(2,t13.collect_item_3d+1) as bigint))),',',
        concat('uc2_collect_item_1d','_',if(t13.collect_item_1d is null, 'null', cast(log(2,t13.collect_item_1d+1) as bigint))),',',
        concat('uc2_cart_item_90d','_',if(t13.cart_item_90d is null, 'null', cast(log(2,t13.cart_item_90d+1) as bigint))),',',
        concat('uc2_cart_item_60d','_',if(t13.cart_item_60d is null, 'null', cast(log(2,t13.cart_item_60d+1) as bigint))),',',
        concat('uc2_cart_item_30d','_',if(t13.cart_item_30d is null, 'null', cast(log(2,t13.cart_item_30d+1) as bigint))),',',
        concat('uc2_cart_item_15d','_',if(t13.cart_item_15d is null, 'null', cast(log(2,t13.cart_item_15d+1) as bigint))),',',
        concat('uc2_cart_item_7d','_',if(t13.cart_item_7d is null, 'null', cast(log(2,t13.cart_item_7d+1) as bigint))),',',
        concat('uc2_cart_item_3d','_',if(t13.cart_item_3d is null, 'null', cast(log(2,t13.cart_item_3d+1) as bigint))),',',
        concat('uc2_cart_item_1d','_',if(t13.cart_item_1d is null, 'null', cast(log(2,t13.cart_item_1d+1) as bigint))),',',
        concat('uc2_cart_num_90d','_',if(t13.cart_num_90d is null, 'null', cast(log(2,t13.cart_num_90d+1) as bigint))),',',
        concat('uc2_cart_num_60d','_',if(t13.cart_num_60d is null, 'null', cast(log(2,t13.cart_num_60d+1) as bigint))),',',
        concat('uc2_cart_num_30d','_',if(t13.cart_num_30d is null, 'null', cast(log(2,t13.cart_num_30d+1) as bigint))),',',
        concat('uc2_cart_num_15d','_',if(t13.cart_num_15d is null, 'null', cast(log(2,t13.cart_num_15d+1) as bigint))),',',
        concat('uc2_cart_num_7d','_',if(t13.cart_num_7d is null, 'null', cast(log(2,t13.cart_num_7d+1) as bigint))),',',
        concat('uc2_cart_num_3d','_',if(t13.cart_num_3d is null, 'null', cast(log(2,t13.cart_num_3d+1) as bigint))),',',
        concat('uc2_cart_num_1d','_',if(t13.cart_num_1d is null, 'null', cast(log(2,t13.cart_num_1d+1) as bigint))),',',
        concat('uc2_buy_item_90d','_',if(t13.buy_item_90d is null, 'null', cast(log(2,t13.buy_item_90d+1) as bigint))),',',
        concat('uc2_buy_item_60d','_',if(t13.buy_item_60d is null, 'null', cast(log(2,t13.buy_item_60d+1) as bigint))),',',
        concat('uc2_buy_item_30d','_',if(t13.buy_item_30d is null, 'null', cast(log(2,t13.buy_item_30d+1) as bigint))),',',
        concat('uc2_buy_item_15d','_',if(t13.buy_item_15d is null, 'null', cast(log(2,t13.buy_item_15d+1) as bigint))),',',
        concat('uc2_buy_item_7d','_',if(t13.buy_item_7d is null, 'null', cast(log(2,t13.buy_item_7d+1) as bigint))),',',
        concat('uc2_buy_item_3d','_',if(t13.buy_item_3d is null, 'null', cast(log(2,t13.buy_item_3d+1) as bigint))),',',
        concat('uc2_buy_item_1d','_',if(t13.buy_item_1d is null, 'null', cast(log(2,t13.buy_item_1d+1) as bigint))),',',
        concat('uc2_buy_num_90d','_',if(t13.buy_num_90d is null, 'null', cast(log(2,t13.buy_num_90d+1) as bigint))),',',
        concat('uc2_buy_num_60d','_',if(t13.buy_num_60d is null, 'null', cast(log(2,t13.buy_num_60d+1) as bigint))),',',
        concat('uc2_buy_num_30d','_',if(t13.buy_num_30d is null, 'null', cast(log(2,t13.buy_num_30d+1) as bigint))),',',
        concat('uc2_buy_num_15d','_',if(t13.buy_num_15d is null, 'null', cast(log(2,t13.buy_num_15d+1) as bigint))),',',
        concat('uc2_buy_num_7d','_',if(t13.buy_num_7d is null, 'null', cast(log(2,t13.buy_num_7d+1) as bigint))),',',
        concat('uc2_buy_num_3d','_',if(t13.buy_num_3d is null, 'null', cast(log(2,t13.buy_num_3d+1) as bigint))),',',
        concat('uc2_buy_num_1d','_',if(t13.buy_num_1d is null, 'null', cast(log(2,t13.buy_num_1d+1) as bigint)))
    )) as user_cate2_cross_feature
    ,if(t14.user_id is null, 'ub_null',concat(
        concat('ub_click_item_90d','_',if(t14.click_item_90d is null, 'null', cast(log(2,t14.click_item_90d+1) as bigint))),',',
        concat('ub_click_item_60d','_',if(t14.click_item_60d is null, 'null', cast(log(2,t14.click_item_60d+1) as bigint))),',',
        concat('ub_click_item_30d','_',if(t14.click_item_30d is null, 'null', cast(log(2,t14.click_item_30d+1) as bigint))),',',
        concat('ub_click_item_15d','_',if(t14.click_item_15d is null, 'null', cast(log(2,t14.click_item_15d+1) as bigint))),',',
        concat('ub_click_item_7d','_',if(t14.click_item_7d is null, 'null', cast(log(2,t14.click_item_7d+1) as bigint))),',',
        concat('ub_click_item_3d','_',if(t14.click_item_3d is null, 'null', cast(log(2,t14.click_item_3d+1) as bigint))),',',
        concat('ub_click_item_1d','_',if(t14.click_item_1d is null, 'null', cast(log(2,t14.click_item_1d+1) as bigint))),',',
        concat('ub_click_num_90d','_',if(t14.click_num_90d is null, 'null', cast(log(2,t14.click_num_90d+1) as bigint))),',',
        concat('ub_click_num_60d','_',if(t14.click_num_60d is null, 'null', cast(log(2,t14.click_num_60d+1) as bigint))),',',
        concat('ub_click_num_30d','_',if(t14.click_num_30d is null, 'null', cast(log(2,t14.click_num_30d+1) as bigint))),',',
        concat('ub_click_num_15d','_',if(t14.click_num_15d is null, 'null', cast(log(2,t14.click_num_15d+1) as bigint))),',',
        concat('ub_click_num_7d','_',if(t14.click_num_7d is null, 'null', cast(log(2,t14.click_num_7d+1) as bigint))),',',
        concat('ub_click_num_3d','_',if(t14.click_num_3d is null, 'null', cast(log(2,t14.click_num_3d+1) as bigint))),',',
        concat('ub_click_num_1d','_',if(t14.click_num_1d is null, 'null', cast(log(2,t14.click_num_1d+1) as bigint))),',',
        concat('ub_click_day_90d','_',if(t14.click_day_90d is null, 'null', cast(log(2,t14.click_day_90d+1) as bigint))),',',
        concat('ub_click_day_60d','_',if(t14.click_day_60d is null, 'null', cast(log(2,t14.click_day_60d+1) as bigint))),',',
        concat('ub_click_day_30d','_',if(t14.click_day_30d is null, 'null', cast(log(2,t14.click_day_30d+1) as bigint))),',',
        concat('ub_click_day_15d','_',if(t14.click_day_15d is null, 'null', cast(log(2,t14.click_day_15d+1) as bigint))),',',
        concat('ub_click_day_7d','_',if(t14.click_day_7d is null, 'null', cast(log(2,t14.click_day_7d+1) as bigint))),',',
        concat('ub_click_day_3d','_',if(t14.click_day_3d is null, 'null', cast(log(2,t14.click_day_3d+1) as bigint))),',',
        concat('ub_click_day_1d','_',if(t14.click_day_1d is null, 'null', cast(log(2,t14.click_day_1d+1) as bigint))),',',
        concat('ub_collect_item_90d','_',if(t14.collect_item_90d is null, 'null', cast(log(2,t14.collect_item_90d+1) as bigint))),',',
        concat('ub_collect_item_60d','_',if(t14.collect_item_60d is null, 'null', cast(log(2,t14.collect_item_60d+1) as bigint))),',',
        concat('ub_collect_item_30d','_',if(t14.collect_item_30d is null, 'null', cast(log(2,t14.collect_item_30d+1) as bigint))),',',
        concat('ub_collect_item_15d','_',if(t14.collect_item_15d is null, 'null', cast(log(2,t14.collect_item_15d+1) as bigint))),',',
        concat('ub_collect_item_7d','_',if(t14.collect_item_7d is null, 'null', cast(log(2,t14.collect_item_7d+1) as bigint))),',',
        concat('ub_collect_item_3d','_',if(t14.collect_item_3d is null, 'null', cast(log(2,t14.collect_item_3d+1) as bigint))),',',
        concat('ub_collect_item_1d','_',if(t14.collect_item_1d is null, 'null', cast(log(2,t14.collect_item_1d+1) as bigint))),',',
        concat('ub_cart_item_90d','_',if(t14.cart_item_90d is null, 'null', cast(log(2,t14.cart_item_90d+1) as bigint))),',',
        concat('ub_cart_item_60d','_',if(t14.cart_item_60d is null, 'null', cast(log(2,t14.cart_item_60d+1) as bigint))),',',
        concat('ub_cart_item_30d','_',if(t14.cart_item_30d is null, 'null', cast(log(2,t14.cart_item_30d+1) as bigint))),',',
        concat('ub_cart_item_15d','_',if(t14.cart_item_15d is null, 'null', cast(log(2,t14.cart_item_15d+1) as bigint))),',',
        concat('ub_cart_item_7d','_',if(t14.cart_item_7d is null, 'null', cast(log(2,t14.cart_item_7d+1) as bigint))),',',
        concat('ub_cart_item_3d','_',if(t14.cart_item_3d is null, 'null', cast(log(2,t14.cart_item_3d+1) as bigint))),',',
        concat('ub_cart_item_1d','_',if(t14.cart_item_1d is null, 'null', cast(log(2,t14.cart_item_1d+1) as bigint))),',',
        concat('ub_cart_num_90d','_',if(t14.cart_num_90d is null, 'null', cast(log(2,t14.cart_num_90d+1) as bigint))),',',
        concat('ub_cart_num_60d','_',if(t14.cart_num_60d is null, 'null', cast(log(2,t14.cart_num_60d+1) as bigint))),',',
        concat('ub_cart_num_30d','_',if(t14.cart_num_30d is null, 'null', cast(log(2,t14.cart_num_30d+1) as bigint))),',',
        concat('ub_cart_num_15d','_',if(t14.cart_num_15d is null, 'null', cast(log(2,t14.cart_num_15d+1) as bigint))),',',
        concat('ub_cart_num_7d','_',if(t14.cart_num_7d is null, 'null', cast(log(2,t14.cart_num_7d+1) as bigint))),',',
        concat('ub_cart_num_3d','_',if(t14.cart_num_3d is null, 'null', cast(log(2,t14.cart_num_3d+1) as bigint))),',',
        concat('ub_cart_num_1d','_',if(t14.cart_num_1d is null, 'null', cast(log(2,t14.cart_num_1d+1) as bigint))),',',
        concat('ub_buy_item_90d','_',if(t14.buy_item_90d is null, 'null', cast(log(2,t14.buy_item_90d+1) as bigint))),',',
        concat('ub_buy_item_60d','_',if(t14.buy_item_60d is null, 'null', cast(log(2,t14.buy_item_60d+1) as bigint))),',',
        concat('ub_buy_item_30d','_',if(t14.buy_item_30d is null, 'null', cast(log(2,t14.buy_item_30d+1) as bigint))),',',
        concat('ub_buy_item_15d','_',if(t14.buy_item_15d is null, 'null', cast(log(2,t14.buy_item_15d+1) as bigint))),',',
        concat('ub_buy_item_7d','_',if(t14.buy_item_7d is null, 'null', cast(log(2,t14.buy_item_7d+1) as bigint))),',',
        concat('ub_buy_item_3d','_',if(t14.buy_item_3d is null, 'null', cast(log(2,t14.buy_item_3d+1) as bigint))),',',
        concat('ub_buy_item_1d','_',if(t14.buy_item_1d is null, 'null', cast(log(2,t14.buy_item_1d+1) as bigint))),',',
        concat('ub_buy_num_90d','_',if(t14.buy_num_90d is null, 'null', cast(log(2,t14.buy_num_90d+1) as bigint))),',',
        concat('ub_buy_num_60d','_',if(t14.buy_num_60d is null, 'null', cast(log(2,t14.buy_num_60d+1) as bigint))),',',
        concat('ub_buy_num_30d','_',if(t14.buy_num_30d is null, 'null', cast(log(2,t14.buy_num_30d+1) as bigint))),',',
        concat('ub_buy_num_15d','_',if(t14.buy_num_15d is null, 'null', cast(log(2,t14.buy_num_15d+1) as bigint))),',',
        concat('ub_buy_num_7d','_',if(t14.buy_num_7d is null, 'null', cast(log(2,t14.buy_num_7d+1) as bigint))),',',
        concat('ub_buy_num_3d','_',if(t14.buy_num_3d is null, 'null', cast(log(2,t14.buy_num_3d+1) as bigint))),',',
        concat('ub_buy_num_1d','_',if(t14.buy_num_1d is null, 'null', cast(log(2,t14.buy_num_1d+1) as bigint)))
    )) as user_brand_cross_feature


from (
    select *
    from user_pay_sample
    where ds=${bizdate}
)t1 left join (
    select user_id, brand_id_seq
    from user_click_brand_seq_feature
    where ds=${bizdate}
)t2 on t1.user_id=t2.user_id
left join (
    select user_id, brand_id_seq
    from user_collect_brand_seq_feature
    where ds=${bizdate}
)t3 on t1.user_id=t3.user_id
left join (
    select user_id, brand_id_seq
    from user_pay_brand_seq_feature
    where ds=${bizdate}
)t4 on t1.user_id=t4.user_id
left join (
    select user_id, cate_seq
    from user_click_cate_seq_feature
    where ds=${bizdate}
)t5 on t1.user_id=t5.user_id
left join (
    select user_id, cate_seq
    from user_collect_cate_seq_feature
    where ds=${bizdate}
)t6 on t1.user_id=t6.user_id
left join (
    select user_id, cate_seq
    from user_pay_cate_seq_feature
    where ds=${bizdate}
)t7 on t1.user_id=t7.user_id

left join (
    select *
    from user_click_beh_feature_ads
    where ds=${bizdate}
)t8 on t1.user_id=t8.user_id
left join (
    select *
    from user_collect_beh_feature_ads
    where ds=${bizdate}
)t9 on t1.user_id=t9.user_id
left join (
    select *
    from user_cart_beh_feature_ads
    where ds=${bizdate}
)t10 on t1.user_id=t10.user_id
left join (
    select *
    from user_buy_beh_feature_ads
    where ds=${bizdate}
)t11 on t1.user_id=t11.user_id
left join (
    select *
    from brand_stat_feature_ads
    where ds=${bizdate}
)t12 on t1.brand_id=t12.brand_id
left join (
    select *
    from user_brand_cate2_cross_beh_feature_ads
    where ds=${bizdate}
)t13 on t1.user_id=t13.user_id and t1.brand_id=t13.brand_id
left join (
    select *
    from user_brand_cross_beh_feature_ads
    where ds=${bizdate}
)t14 on t1.user_id=t14.user_id and t1.brand_id=t14.brand_id
where (t2.brand_id_seq is not null or t3.brand_id_seq is not null or t4.brand_id_seq is not null or
    t5.cate_seq is not null or t6.cate_seq is not null or t7.cate_seq is not null or
    t8.cnt_days_90d is not null or t9.cate1_num_90d is not null or t10.item_num_90d is not null
    or t11.item_num_90d is not null or t12.click_num is not null or t13.click_item_90d is not null or
    t14.click_item_90d is not null)
;


SELECT count(*) from user_pay_sample_feature_join_dnn  ;


--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-11 21:51:20
--********************************************************************--
create table if not exists user_pay_sample_feature_seq (
    feature string ,
    number bigint
)LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_seq
select feature, ROW_NUMBER() OVER(ORDER BY feature) AS number
from (
    select DISTINCT feature
    from (
        select target_brand_id as feature
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',click_brand_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',collect_brand_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',pay_brand_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',click_cate_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',collect_cate_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',pay_cate_seq) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_click_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_collect_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_cart_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_pay_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',brand_stat_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_cate2_cross_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
        union all
        select trans_array(0,',',user_brand_cross_feature) as (feature)
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
    )t1
)t1
;

select * from user_pay_sample_feature_seq limit 4;

create table if not exists user_pay_sample_feature_join_dnn_seq(
    user_id string,
    brand_id string,
    label bigint,
    bizdate string,
    target_brand_id string,
    click_brand_seq string,
    collect_brand_seq string,
    pay_brand_seq string,
    click_cate_seq string,
    collect_cate_seq string,
    pay_cate_seq string,
    user_click_feature string,
    user_collect_feature string,
    user_cart_feature string,
    user_pay_feature string,
    brand_stat_feature string,
    user_cate2_cross_feature string,
    user_brand_cross_feature string
)LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_join_dnn_seq
select t1.user_id, t1.brand_id, t1.label, t1.ds, t1.number,
    t2.feature, t3.feature, t5.feature, t6.feature, t7.feature, t8.feature
    ,t9.feature, t10.feature, t11.feature, t12.feature, t13.feature, t14.feature, t15.feature
from (
    select t1.user_id, t1.brand_id, t1.label, t1.ds, t2.number
    from (
        select user_id, brand_id, label, ds, target_brand_id
        from user_pay_sample_feature_join_dnn
        where ds>='20130701' and ds<='20130916'
    )t1 join (
        select feature, number
        from user_pay_sample_feature_seq
    )t2 on t1.target_brand_id=t2.feature
)t1 join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, click_brand_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t2 on t1.user_id=t2.user_id and t1.brand_id=t2.brand_id and t1.label=t2.label and t1.ds=t2.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, collect_brand_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t3 on t1.user_id=t3.user_id and t1.brand_id=t3.brand_id and t1.label=t3.label and t1.ds=t3.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, pay_brand_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t5 on t1.user_id=t5.user_id and t1.brand_id=t5.brand_id and t1.label=t5.label and t1.ds=t5.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, click_cate_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t6 on t1.user_id=t6.user_id and t1.brand_id=t6.brand_id and t1.label=t6.label and t1.ds=t6.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, collect_cate_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t7 on t1.user_id=t7.user_id and t1.brand_id=t7.brand_id and t1.label=t7.label and t1.ds=t7.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, pay_cate_seq) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t8 on t1.user_id=t8.user_id and t1.brand_id=t8.brand_id and t1.label=t8.label and t1.ds=t8.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_click_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t9 on t1.user_id=t9.user_id and t1.brand_id=t9.brand_id and t1.label=t9.label and t1.ds=t9.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_collect_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t10 on t1.user_id=t10.user_id and t1.brand_id=t10.brand_id and t1.label=t10.label and t1.ds=t10.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_cart_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t11 on t1.user_id=t11.user_id and t1.brand_id=t11.brand_id and t1.label=t11.label and t1.ds=t11.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_pay_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t12 on t1.user_id=t12.user_id and t1.brand_id=t12.brand_id and t1.label=t12.label and t1.ds=t12.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, brand_stat_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t13 on t1.user_id=t13.user_id and t1.brand_id=t13.brand_id and t1.label=t13.label and t1.ds=t13.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_cate2_cross_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t14 on t1.user_id=t14.user_id and t1.brand_id=t14.brand_id and t1.label=t14.label and t1.ds=t14.ds
join (
    select user_id, brand_id, label, ds, WM_CONCAT(',', number) as feature
    from (
        select user_id, brand_id, label, ds, number
        from (
            select trans_array(4, ',', user_id, brand_id, label, ds, user_brand_cross_feature) as (user_id, brand_id, label, ds, feature)
            from user_pay_sample_feature_join_dnn
            where ds>='20130701' and ds<='20130916'
        )t1 join (
            select feature, number
            from user_pay_sample_feature_seq
        )t2 on t1.feature=t2.feature
    )t1
    group by user_id, brand_id, label, ds
)t15 on t1.user_id=t15.user_id and t1.brand_id=t15.brand_id and t1.label=t15.label and t1.ds=t15.ds
;


SELECT * from user_pay_sample_feature_join_dnn_seq limit 4; 

create table if not exists user_pay_sample_feature_join_dnn_seq_shuffle(
    key_all string,
    label bigint,
    target_brand_id string,
    click_brand_seq string,
    collect_brand_seq string,
    pay_brand_seq string,
    click_cate_seq string,
    collect_cate_seq string,
    pay_cate_seq string,
    user_click_feature string,
    user_collect_feature string,
    user_cart_feature string,
    user_pay_feature string,
    brand_stat_feature string,
    user_cate2_cross_feature string,
    user_brand_cross_feature string
)LIFECYCLE 90;

insert OVERWRITE TABLE user_pay_sample_feature_join_dnn_seq_shuffle
select key_all, max(label), MAX(target_brand_id), MAX(click_brand_seq), MAX(collect_brand_seq), MAX(pay_brand_seq)
    ,max(click_cate_seq), max(collect_cate_seq), max(pay_cate_seq), max(user_click_feature)
    ,max(user_collect_feature), max(user_cart_feature), max(user_pay_feature), max(brand_stat_feature)
    ,max(user_cate2_cross_feature), max(user_brand_cross_feature)
from (
    select *, concat(RAND(),'_',RAND(),'_',user_id,'_',brand_id,'_',label,'_',bizdate) as key_all
    from user_pay_sample_feature_join_dnn_seq
)t1 group by key_all
;

-- select count(*) from user_pay_sample_feature_join_dnn_seq_shuffle limit 100;
--         select count(*)
--         from user_pay_sample_feature_join
--         where ds>='20130701' and ds<='20130916'
-- ;
SELECT count(*) FROM user_pay_sample_feature_join_dnn_seq_shuffle ; 

select * from user_pay_sample_feature_join_dnn_seq_shuffle limit 1;
