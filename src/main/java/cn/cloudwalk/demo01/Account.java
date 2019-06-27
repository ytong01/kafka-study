package cn.cloudwalk.demo01;

import java.io.Serializable;

public class Account implements Serializable {

    private String id;
    private String phone;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

}
