package cn.cloudwalk.demo05.validation;

import cn.cloudwalk.demo05.domain.NetworkSignalDomain;

public class NetworkSignalValidator {

    public boolean isValid(NetworkSignalDomain entity) {
        return entity.getId() != null && entity.getDeviceId() != null && entity.getTime() != null &&
                entity.getTxSpeed() != null && entity.getRxSpeed() != null && entity.getTxData() != null && entity.getRxData() != null &&
                entity.getRxData() > 0 && entity.getTxData() > 0 && entity.getRxSpeed() > 0 && entity.getTxSpeed() > 0;

    }
}
