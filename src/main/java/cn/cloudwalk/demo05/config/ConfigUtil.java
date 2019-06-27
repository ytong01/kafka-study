package cn.cloudwalk.demo05.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {

    public static InputStream getResource(String resourceName) {
        return ConfigUtil.class.getClassLoader().getResourceAsStream(resourceName);
    }

    public static Properties getConfig(String configName) {
        Properties props = new Properties();
        try {
            props.load(getResource(configName + ".properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props;
    }
}
