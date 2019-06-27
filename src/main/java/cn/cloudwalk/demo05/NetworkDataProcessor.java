package cn.cloudwalk.demo05;

import cn.cloudwalk.demo05.config.ConfigUtil;
import cn.cloudwalk.demo05.processor.NetworkDataKafkaProcessor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class NetworkDataProcessor implements Runnable {

    private static Processor<byte[], byte[]> getProcessor() {
        return new NetworkDataKafkaProcessor();
    }

    @Override
    public void run() {

        Properties properties = ConfigUtil.getConfig("network-data");
        String topics = properties.getProperty("topic.names");
        StreamsConfig config = new StreamsConfig(properties);

        TopologyBuilder builder = new TopologyBuilder().addSource("SOURCE", topics.split(","))
                                                       .addProcessor("PROCESSOR", NetworkDataProcessor::getProcessor, "SOURCE");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}