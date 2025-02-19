--odps sql 
--********************************************************************--
--author:zhng25
--create time:2025-02-06 16:23:57
--********************************************************************--
CREATE  TABLE if not exists user_item_beh_log (
    item_id string, 
    user_id string,
    action string,
    vtime string
) LIFECYCLE 90;

SELECT max(vtime), min(vtime), count(DISTINCT item_id), count(DISTINCT user_id), count(*)
FROM user_item_beh_log ;

SELECT DISTINCT action from user_item_beh_log  ;

select  count(*) from user_item_beh_log where action='alipay';
