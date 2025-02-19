
# 模型的config设置    接近2w条特征
config = {
    "embedding_dim": 32,
    "num_embedding": 20000,
    "lr": 0.004,
    "batch_size": 512,
    "num_experts": 3,
    "feature_col": ["target_brand_id","click_brand_seq","collect_brand_seq","pay_brand_seq","click_cate_seq","collect_cate_seq","pay_cate_seq",
                   "user_click_feature","user_collect_feature","user_cart_feature","user_pay_feature","brand_stat_feature","user_cate2_cross_feature","user_brand_cross_feature"],
    "features_gate_col": ["target_brand_id","brand_stat_feature","click_brand_seq","user_cate2_cross_feature","user_brand_cross_feature"]
}