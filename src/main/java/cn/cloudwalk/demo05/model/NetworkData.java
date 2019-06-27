package cn.cloudwalk.demo05.model;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.util.List;

public class NetworkData implements Serializable {

    private String deviceId;

    private Long time;

    private List<NetworkSignal> signalList;

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getDeviceId() {

        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public List<NetworkSignal> getSignalList() {
        return signalList;
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }

    public void setSignalList(List<NetworkSignal> signalList) {
        this.signalList = signalList;
    }
}
