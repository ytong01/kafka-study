package cn.cloudwalk.demo05;

import cn.cloudwalk.demo05.config.ConfigUtil;
import cn.cloudwalk.demo05.model.NetworkData;
import cn.cloudwalk.demo05.model.NetworkSignal;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomNetworkDataProducer implements Runnable {

    private static final long INCOMING_DATA_INTERVAL = 500;

    @Override
    public void run() {

        Properties props = ConfigUtil.getConfig("network-data");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put("ack", "all");

        String topic = props.getProperty("topic.names");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int deviceCount = 100;
        List<String> deviceIds = IntStream.range(0, deviceCount).mapToObj(value -> UUID.randomUUID().toString()).collect(Collectors.toList());
        Random random = new Random();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {

            NetworkData networkData = new NetworkData();
            networkData.setDeviceId(deviceIds.get(random.nextInt(deviceCount)));
            networkData.setSignalList(new ArrayList<>());
            for (int j = 0; j < random.nextInt(4) + 1; j++) {
                NetworkSignal networkSignal = new NetworkSignal();
                networkSignal.setNetworkType(i % 2 == 0 ? "4G" : "wifi");
                networkSignal.setRxData((long) random.nextInt(1000));
                networkSignal.setTxData((long) random.nextInt(1000));
                networkSignal.setRxSpeed((double) random.nextInt(100));
                networkSignal.setTxSpeed((double) random.nextInt(100));
                networkSignal.setTime(System.currentTimeMillis());
                networkData.getSignalList().add(networkSignal);
            }

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + System.currentTimeMillis(), networkData.toString());
            producer.send(record);
            try {
                Thread.sleep(INCOMING_DATA_INTERVAL);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
