package cn.cloudwalk.demo05.domain;

import com.alibaba.fastjson.JSONObject;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

public class NetworkSignalDomain implements Serializable {

    @QuerySqlField
    private String id;

    @QuerySqlField(index = true)
    private String deviceId;

    @QuerySqlField(index = true)
    private Long time;

    @QuerySqlField(index = true)
    private String networkType;

    @QuerySqlField
    private Double rxSpeed;

    @QuerySqlField
    private Double txSpeed;

    @QuerySqlField
    private Double latitude;

    @QuerySqlField
    private Double longitude;

    @QuerySqlField
    private Long txData;

    @QuerySqlField
    private Long rxData;

    public Double getTxSpeed() {
        return txSpeed;
    }

    public void setTxSpeed(Double txSpeed) {
        this.txSpeed = txSpeed;
    }

    public Long getTxData() {
        return txData;
    }

    public void setTxData(Long txData) {
        this.txData = txData;
    }

    public Long getRxData() {
        return rxData;
    }

    public void setRxData(Long rxData) {
        this.rxData = rxData;
    }

    public String getId() {

        return id;

    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getNetworkType() {
        return networkType;
    }

    public Long getTime() {
        return time;
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public void setNetworkType(String networkType) {
        this.networkType = networkType;
    }

    public Double getRxSpeed() {
        return rxSpeed;
    }

    public void setRxSpeed(Double rxSpeed) {
        this.rxSpeed = rxSpeed;
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
