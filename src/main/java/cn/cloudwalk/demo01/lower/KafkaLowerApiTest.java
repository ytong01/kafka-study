package cn.cloudwalk.demo01.lower;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class KafkaLowerApiTest {

    public static void main(String[] args) throws InterruptedException {

        JavaKafkaSimpleConsumerAPI api = new JavaKafkaSimpleConsumerAPI();
        long maxReads = 300;
        String topic = "ip_login";
        int partitionID = 0;

        KafkaTopicPartitionInfo partitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
        List<KafkaBrokerInfo> brokerInfoList = Arrays.asList(new KafkaBrokerInfo("10.129.12.55", 9092));

        api.run(maxReads, partitionInfo, brokerInfoList);

    }
}
