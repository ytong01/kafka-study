package cn.cloudwalk.demo05.model;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public class NetworkSignal implements Serializable {

    private Long time;
    private String networkType;
    private Long rxData;
    private Long txData;
    private Double rxSpeed;

    private Double txSpeed;

    private Double latitude;

    private Double longitude;

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }

    public String getNetworkType() {
        return networkType;
    }

    public void setNetworkType(String networkType) {
        this.networkType = networkType;
    }

    public Long getRxData() {
        return rxData;
    }

    public void setRxData(Long rxData) {
        this.rxData = rxData;
    }

    public Long getTxData() {
        return txData;
    }

    public void setTxData(Long txData) {
        this.txData = txData;
    }

    public Double getRxSpeed() {
        return rxSpeed;
    }

    public void setRxSpeed(Double rxSpeed) {
        this.rxSpeed = rxSpeed;
    }

    public Double getTxSpeed() {
        return txSpeed;
    }

    public void setTxSpeed(Double txSpeed) {
        this.txSpeed = txSpeed;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }
}
