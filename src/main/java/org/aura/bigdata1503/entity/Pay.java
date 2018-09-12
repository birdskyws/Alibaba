package org.aura.bigdata1503.entity;

import java.io.Serializable;

public class Pay implements Serializable {
    private Long user_id;
    private Long shop_id;

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

    public Pay(Long user_id, Long shop_id) {
        this.user_id = user_id;
        this.shop_id = shop_id;
    }
}
