package org.aura.bigdata1503.entity;


import java.sql.Timestamp;

public class View {
    private Long user_id;
    private Long shop_id;
    private String ts;

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Long getShop_id() {
        return shop_id;
    }

    public void setShop_id(Long shop_id) {
        this.shop_id = shop_id;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public View(Long user_id, Long shop_id, String ts) {
        this.user_id = user_id;
        this.shop_id = shop_id;
        this.ts = ts;
    }
}
