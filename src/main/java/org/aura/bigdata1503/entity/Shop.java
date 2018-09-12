package org.aura.bigdata1503.entity;

import java.io.Serializable;

public class Shop implements Serializable {
    private Long shop_id;
    private String city_name;
    private String location_id;
    private int pre_pay;
    private int score;
    private String comment_cnt;
    private String shop_level;
    private String cate_1_name;
    private String cate_2_name;
    private String cate_3_name;

    public Long getShop_id() {
        return shop_id;
    }

    public void setShop_id(Long shop_id) {
        this.shop_id = shop_id;
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = city_name;
    }

    public String getLocation_id() {
        return location_id;
    }

    public void setLocation_id(String location_id) {
        this.location_id = location_id;
    }

    public int getPre_pay() {
        return pre_pay;
    }

    public void setPre_pay(int pre_pay) {

        this.pre_pay = pre_pay;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getComment_cnt() {
        return comment_cnt;
    }

    public void setComment_cnt(String comment_cnt) {
        this.comment_cnt = comment_cnt;
    }

    public String getShop_level() {
        return shop_level;
    }

    public void setShop_level(String shop_level) {
        this.shop_level = shop_level;
    }

    public String getCate_1_name() {
        return cate_1_name;
    }

    public void setCate_1_name(String cate_1_name) {
        this.cate_1_name = cate_1_name;
    }

    public String getCate_2_name() {
        return cate_2_name;
    }

    public void setCate_2_name(String cate_2_name) {
        this.cate_2_name = cate_2_name;
    }

    public String getCate_3_name() {
        return cate_3_name;
    }

    public void setCate_3_name(String cate_3_name) {
        this.cate_3_name = cate_3_name;
    }

    public Shop(Long shop_id, String city_name, String location_id, String pre_pay, String score, String comment_cnt, String shop_level, String cate_1_name, String cate_2_name, String cate_3_name) {
        this.shop_id = shop_id;
        this.city_name = city_name;
        this.location_id = location_id;
        if(pre_pay.equals(""))
            this.pre_pay = 0;
        else
            this.pre_pay = Integer.parseInt(pre_pay);
        if(score.equals(""))
            this.score = 0;
        else
            this.score = Integer.parseInt(score);
        this.comment_cnt = comment_cnt;
        this.shop_level = shop_level;
        this.cate_1_name = cate_1_name;
        this.cate_2_name = cate_2_name;
        this.cate_3_name = cate_3_name;
    }
}

