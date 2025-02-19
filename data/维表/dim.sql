--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-07 14:18:47
--********************************************************************--

--只选择头部品牌
CREATE TABLE if not EXISTS brand_top500_buy_num(
    brand_id STRING ,
    buy_num bigint
) LIFECYCLE 90;

INSERT OVERWRITE TABLE brand_top500_buy_num
SELECT brand_id, buy_num
FROM (
    SELECT brand_id, count(DISTINCT user_id) as buy_num
    FROM dw_user_item_buy_log 
    WHERE ds <= ${bizdate} and ds >= to_char(dateadd(TO_DATE(${bizdate}, 'yyyymmdd'), -30, 'dd'), 'yyyymmdd')
        and brand_id is not null
    GROUP BY brand_id
) t1 order by buy_num desc limit 500
;

SELECT * from brand_top500_buy_num ;

-- 品牌top2二级类目
CREATE  table if not EXISTS brand_cate2_dim(
    brand_id STRING ,
    cate2 string
)LIFECYCLE 90;

INSERT OVERWRITE TABLE brand_cate2_dim
SELECT brand_id, cate2
FROM (
    SELECT brand_id, cate2, ROW_NUMBER() OVER (PARTITION BY brand_id order by num desc) as number 
    FROM (
        SELECT brand_id, cate2, count(DISTINCT user_id) as num 
        FROM (
            SELECT t1.user_id, t1.brand_id, t1.cate2 
            FROM (
                SELECT user_id, brand_id, cate2 
                FROM dw_user_item_buy_log 
                WHERE ds <= ${bizdate} and ds >= to_char(dateadd(TO_DATE(${bizdate}, 'yyyymmdd'), -30, 'dd'), 'yyyymmdd')
            ) t1 join(
                SELECT brand_id
                FROM brand_top500_buy_num 
            ) t2 on t1.brand_id=t2.brand_id
        )t1 GROUP BY brand_id, cate2
    ) 
)t1 where number <= 2;

SELECT count(*) from brand_cate2_dim ;


--top 10000
CREATE TABLE if not EXISTS brand_top10000_buy_num(
    brand_id STRING ,
    buy_num bigint
) LIFECYCLE 90;

INSERT OVERWRITE TABLE brand_top10000_buy_num
SELECT brand_id, buy_num
FROM (
    SELECT brand_id, count(DISTINCT user_id) as buy_num
    FROM dw_user_item_buy_log 
    WHERE ds <= ${bizdate} and ds >= to_char(dateadd(TO_DATE(${bizdate}, 'yyyymmdd'), -30, 'dd'), 'yyyymmdd')
        and brand_id is not null
    GROUP BY brand_id
) t1 order by buy_num desc limit 10000
;


SELECT count(DISTINCT cate2) from brand_cate2_dim ;


--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-06 14:40:18
--********************************************************************--
CREATE TABLE if not EXISTS item_dim (
    item_id string, 
    title string,
    pict_url string,
    category string,
    brand_id string,
    seller_id string
) LIFECYCLE 90;

SELECT * FROM item_dim  limit 10;


--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-08 13:21:31
--********************************************************************--
create table if not EXISTS user_id_number(
    user_id string,
    number bigint
) LIFECYCLE 90;

INSERT OVERWRITE TABLE user_id_number
SELECT user_id, ROW_NUMBER() OVER (ORDER BY rnd DESC) as number
FROM (
    SELECT user_id, RAND() as rnd
    FROM (
        SELECT distinct user_id
        FROM user_item_beh_log 
    )
);

SELECT * from user_id_number LIMIT 100;
