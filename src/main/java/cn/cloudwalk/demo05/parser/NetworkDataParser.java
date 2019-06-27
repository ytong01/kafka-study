package cn.cloudwalk.demo05.parser;

import cn.cloudwalk.demo05.domain.NetworkSignalDomain;
import cn.cloudwalk.demo05.model.NetworkData;
import cn.cloudwalk.demo05.model.NetworkSignal;
import cn.cloudwalk.demo05.utils.HashCodeUtil;
import cn.cloudwalk.demo05.validation.NetworkSignalValidator;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NetworkDataParser {

    private NetworkSignalValidator validator;
    public NetworkDataParser(NetworkSignalValidator validator) {

        this.validator = validator;
    }

    public List<NetworkSignalDomain> parser(String json) {

        NetworkData networkData = JSONObject.parseObject(json, NetworkData.class);
        if (networkData == null || networkData.getSignalList() == null) {
            return new ArrayList<>(0);
        }
        return networkData.getSignalList().stream()
                .map(signal -> convert(networkData.getDeviceId(), signal))
                .map(domain -> {
                    domain.setId(generateUniqueId(domain));
                    return domain;
                }).filter(domain -> validator.isValid(domain))
                .collect(Collectors.toList());
    }

    private NetworkSignalDomain convert(String deviceId, NetworkSignal signal) {

        NetworkSignalDomain entity = new NetworkSignalDomain();
        BeanUtils.copyProperties(signal, entity);
        entity.setDeviceId(deviceId);
        return entity;
    }

    /** 生产唯一的Hash值. */
    private String generateUniqueId(NetworkSignalDomain entity) {
        try {
            return HashCodeUtil.getHashString(entity.getDeviceId(), entity.getTime(), entity.getNetworkType(), entity.getRxData(), entity.getTxData(), entity.getRxSpeed(), entity.getTxSpeed(), entity.getLatitude(), entity.getLongitude());
        } catch (Exception e) {
            return null;
        }
    }
}
