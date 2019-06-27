package cn.cloudwalk.demo03;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomerFileStreamSourceConnector extends SourceConnector {

    public static final String TOPIC_CONFIG = "topic";
    public static final String FILE_CONFIG = "file";
    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(FILE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Source filename.")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The topic to publish data to");

    private String filename;
    private String topic;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {

        filename = map.get(FILE_CONFIG);
        topic = map.get(TOPIC_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CustomerFileStreamSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int minTasks) {

        List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        Map<String, String> config = new HashMap<String, String>();
        if (filename != null) {
            config.put(FILE_CONFIG, filename);
        }
        if (topic != null) {
            config.put(TOPIC_CONFIG, topic);
        }
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
