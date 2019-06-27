package cn.cloudwalk.demo01.lower;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;

public class JavaKafkaSimpleConsumerAPI {

    private int maxRetryTimes = 5;
    private long retryIntervalMillis = 1000;

    private Map<KafkaTopicPartitionInfo, List<KafkaBrokerInfo>> replicaBrokers = new HashMap<>();

    public void run(long maxReads, KafkaTopicPartitionInfo topicPartitionInfo, List<KafkaBrokerInfo> seedBrokers) throws InterruptedException {

        long whichTime = kafka.api.OffsetRequest.EarliestTime();
        String clientName = this.createClientName(topicPartitionInfo.topic, topicPartitionInfo.partitionID);
        String groupId = clientName;
        PartitionMetadata partitionMetadata = this.findLeader(seedBrokers, topicPartitionInfo.topic, topicPartitionInfo.partitionID);

        validatePartitionMetadata(partitionMetadata);

        //获取到leader
        SimpleConsumer consumer = this.createSimpleConsumer(partitionMetadata.leader().host(), partitionMetadata.leader().port(), clientName);
        try {
            int times = 0;
            long readOffset = -1;
            while (true) {

                readOffset = this.getLastOffset(consumer, groupId, topicPartitionInfo.topic, topicPartitionInfo.partitionID, whichTime, clientName);
                if (readOffset == -1) {
                    if (times > maxRetryTimes) {
                        throw new RuntimeException("");
                    }
                    times++;
                    Thread.sleep(1000);
                    consumer = this.createNewSimpleConsumer(consumer, topicPartitionInfo.topic, topicPartitionInfo.partitionID);
                    continue;
                }
                break;
            }
            System.out.println("The first read offset is:" + readOffset);

            int numErrors = 0;
            boolean ever = maxReads <= 0;
            while (ever || maxReads > 0) {

                FetchRequest request = new FetchRequestBuilder().clientId(clientName).addFetch(topicPartitionInfo.topic, topicPartitionInfo.partitionID, readOffset, 10000).build();
                FetchResponse response = consumer.fetch(request);
                if (response.hasError()) {
                    numErrors++;
                    if (numErrors > 5) break;
                    short code = response.errorCode(topicPartitionInfo.topic, topicPartitionInfo.partitionID);
                    System.out.println("Error fetching data from the Broker:" + consumer.host() + " Reason:" + code);

                    if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                        readOffset = this.getLastOffset(consumer, groupId, topicPartitionInfo.topic, topicPartitionInfo.partitionID, kafka.api.OffsetRequest.LatestTime(), clientName);
                        continue;
                    }
                    consumer.close();
                    consumer = this.createNewSimpleConsumer(consumer, topicPartitionInfo.topic, topicPartitionInfo.partitionID);
                    continue;
                }
                numErrors = 0;


                long numRead = 0;
                for (MessageAndOffset message : response.messageSet(topicPartitionInfo.topic, topicPartitionInfo.partitionID)) {

                    long currentOffset = message.offset();
                    if (currentOffset < readOffset) {
                        System.out.println("Found and old offset:" + currentOffset + " Expection:" + readOffset);
                        continue;
                    }
                    readOffset = message.nextOffset();
                    ByteBuffer payload = message.message().payload();
                    byte[] data = new byte[payload.limit()];
                    payload.get(data);
                    System.out.println(new String(data, "utf-8"));
                    maxReads--;
                    numRead++;
                }

                consumer = this.updateOffset(consumer, topicPartitionInfo.topic, topicPartitionInfo.partitionID, readOffset, groupId, clientName, 0);
                if (numRead == 0) {
                    Thread.sleep(1000);
                }
            }
            System.out.println("执行完成。。。。");
        } catch (Exception ex) {

        } finally {
            // 关闭资源
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    // nothings
                }
            }
        }
    }

    private SimpleConsumer updateOffset(SimpleConsumer consumer, String topic, int partitionID, long readOffset, String groupId, String clientName, int times) throws InterruptedException {

        Map<TopicAndPartition, OffsetAndMetadata> requestInfoMap = new HashMap<>();
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
        requestInfoMap.put(topicAndPartition, new OffsetAndMetadata(readOffset, OffsetAndMetadata.NoMetadata(), -1));
        OffsetCommitRequest ocRequest = new OffsetCommitRequest(groupId, requestInfoMap, 0, clientName);
        OffsetCommitResponse response = consumer.commitOffsets(ocRequest);

        if (response.hasError()) {
            short code = response.errorCode(topicAndPartition);
            if (times > maxRetryTimes) {

                throw new RuntimeException("Update the Offset occur exception," +
                        " the current response code is:" + code);
            }
            if (code == ErrorMapping.LeaderNotAvailableCode()) {
                Thread.sleep(1000);
                PartitionMetadata partitionMetadata = this.findNewLeaderMetadata(consumer.host(), topic, partitionID);
                this.validatePartitionMetadata(partitionMetadata);
                consumer = this.createSimpleConsumer(partitionMetadata.leader().host(), partitionMetadata.leader().port(), clientName);
                consumer = updateOffset(consumer, topic, partitionID, readOffset, groupId, clientName, times + 1);
            }
            if (code == ErrorMapping.RequestTimedOutCode()) {
                consumer = updateOffset(consumer, topic, partitionID, readOffset, groupId, clientName, times + 1);
            }

            // 其他code直接抛出异常
            throw new RuntimeException("Update the Offset occur exception," +
                    " the current response code is:" + code);
        }
        return consumer;
    }

    private SimpleConsumer createNewSimpleConsumer(SimpleConsumer consumer, String topic, int partitionID) {
        PartitionMetadata partitionMetadata = this.findNewLeaderMetadata(consumer.host(), topic, partitionID);
        this.validatePartitionMetadata(partitionMetadata);
        return this.createSimpleConsumer(partitionMetadata.leader().host(), partitionMetadata.leader().port(), consumer.clientId());
    }

    private PartitionMetadata findNewLeaderMetadata(String oldLeader, String topic, int partitionID) {

        KafkaTopicPartitionInfo topicPartitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
        List<KafkaBrokerInfo> brokerInfos = this.replicaBrokers.get(topicPartitionInfo);
        for (int i = 0; i < 3; i++) {
            boolean gotoSleep = false;
            PartitionMetadata partitionMetadata = this.findLeader(brokerInfos, topic, partitionID);
            if (partitionMetadata == null) {
                gotoSleep = true;
            } else if (partitionMetadata.leader() == null) {
                gotoSleep = true;
            } else if (oldLeader.equalsIgnoreCase(partitionMetadata.leader().host()) && i == 0) {
                gotoSleep = true;
            } else {
                return partitionMetadata;
            }
            if (gotoSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting!!");
        throw new RuntimeException("Unable to find new leader after Broker failure. Exiting!!");
    }

    /**
     * 获取当前groupId对应的consumer在topic和partition中对应的偏移量
     *
     * @param consumer
     * @param groupId
     * @param topic
     * @param partitionID
     * @param whichTime
     * @param clientName
     * @return
     */
    private long getLastOffset(SimpleConsumer consumer, String groupId, String topic, int partitionID, long whichTime, String clientName) {

        long offset = this.getOffsetOfTopicAndPartition(consumer, groupId, clientName, topic, partitionID);
        if (offset > 0) {
            return offset;
        }
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partitionID));
            return -1;
        }
        long[] offsets = response.offsets(topic, partitionID);
        return offsets[0];
    }

    /**
     * 获取topic的partitionID的offset
     *
     * @param consumer
     * @param groupId
     * @param clientName
     * @param topic
     * @param partitionID
     * @return
     */
    private long getOffsetOfTopicAndPartition(SimpleConsumer consumer, String groupId, String clientName, String topic, int partitionID) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
        List<TopicAndPartition> requestInfo = new ArrayList<>();
        requestInfo.add(topicAndPartition);
        OffsetFetchRequest request = new OffsetFetchRequest(groupId, requestInfo, 0, clientName);
        OffsetFetchResponse response = consumer.fetchOffsets(request);
        Map<TopicAndPartition, OffsetMetadataAndError> offsets = response.offsets();
        if (offsets != null && offsets.size() > 0) {
            OffsetMetadataAndError metadata = offsets.get(topicAndPartition);
            if (ErrorMapping.NoError() == metadata.error()) {
                return metadata.offset();
            } else {
                System.out.println("Error fetching data Offset Data the Topic and Partition. Reason: " + metadata.error());
            }
        }
        return 0;
    }

    private SimpleConsumer createSimpleConsumer(String host, int port, String clientName) {

        return new SimpleConsumer(host, port, 10000, 64 * 1024, clientName);
    }

    private void validatePartitionMetadata(PartitionMetadata metadata) {
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting!!");
            throw new IllegalArgumentException("Can't find metadata for Topic and Partition. Exiting!!");
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting!!");
            throw new IllegalArgumentException("Can't find Leader for Topic and Partition. Exiting!!");
        }
    }

    private String createClientName(String topic, int partitionID) {
        return "client_" + topic + "_" + partitionID;
    }

    private PartitionMetadata findLeader(List<KafkaBrokerInfo> brokerInfos, String topic, int partitionID) {

        PartitionMetadata metadata = null;
        for (KafkaBrokerInfo each : brokerInfos) {
            SimpleConsumer consumer = null;

            consumer = new SimpleConsumer(each.brokerHost, each.brokerPort, 10000, 64 * 1024, "leaderLookUp");
            TopicMetadataRequest request = new TopicMetadataRequest(Collections.singletonList(topic));
            TopicMetadataResponse response = consumer.send(request);

            List<TopicMetadata> topicMetadataSeq = response.topicsMetadata();
            for (TopicMetadata topicMetadata : topicMetadataSeq) {

                if (topic.equals(topicMetadata.topic())) {
                    List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionsMetadata();
                    for (PartitionMetadata partitionMetadata : partitionMetadatas) {

                        if (partitionMetadata.partitionId() == partitionID) {

                            KafkaTopicPartitionInfo kafkaTopicPartitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
                            List<KafkaBrokerInfo> brokerInfoList = this.replicaBrokers.get(kafkaTopicPartitionInfo);
                            if (brokerInfoList == null) {
                                brokerInfoList = new ArrayList<>();
                            } else {
                                brokerInfoList.clear();
                            }
                            for (Broker endPoint : partitionMetadata.replicas()) {
                                brokerInfoList.add(new KafkaBrokerInfo(endPoint.host(), endPoint.port()));
                            }

                            this.replicaBrokers.put(kafkaTopicPartitionInfo, brokerInfoList);
                            return partitionMetadata;
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * 从Kafka集群中获取指定topic的分区ID<br/>
     * 如果集群中不存在对应的topic，那么返回一个empty的集合
     *
     * @param brokers    Kafka集群连接参数，eg: {"hadoop-senior01" -> 9092, "hadoop-senior02" -> 9092}
     * @param topic      要获取ID对应的主题
     * @param soTimeout  过期时间
     * @param bufferSize 缓冲区大小
     * @param clientId   client连接ID
     * @return
     */
    public static List<Integer> fetchTopicPartitionIDs(List<KafkaBrokerInfo> brokers, String topic, int soTimeout, int bufferSize, String clientId) {
        Set<Integer> partitionIDs = new HashSet<Integer>();

        List<String> topics = Collections.singletonList(topic);

        // 连接所有的Kafka服务器，然后获取参数 ==> 遍历连接
        for (KafkaBrokerInfo broker : brokers) {
            SimpleConsumer consumer = null;

            try {
                // 构建简单消费者连接对象
                consumer = new SimpleConsumer(broker.brokerHost, broker.brokerPort, soTimeout, bufferSize, clientId);

                // 构建请求参数
                TopicMetadataRequest tmRequest = new TopicMetadataRequest(topics);

                // 发送请求
                TopicMetadataResponse response = consumer.send(tmRequest);

                // 获取返回结果
                List<TopicMetadata> metadatas = response.topicsMetadata();

                // 遍历返回结果，获取对应topic的结果值
                for (TopicMetadata metadata : metadatas) {
                    if (metadata.errorCode() == ErrorMapping.NoError()) {
                        // 没有异常的情况下才进行处理
                        if (topic.equals(metadata.topic())) {
                            // 处理当前topic对应的分区
                            for (PartitionMetadata part : metadata.partitionsMetadata()) {
                                partitionIDs.add(part.partitionId());
                            }
                            // 处理完成，结束循环
                            break;
                        }
                    }
                }
            } finally {
                // 关闭连接
                closeSimpleConsumer(consumer);
            }
        }

        // 返回结果
        return new ArrayList<Integer>(partitionIDs);
    }

    /**
     * 关闭对应资源
     *
     * @param consumer
     */
    private static void closeSimpleConsumer(SimpleConsumer consumer) {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                // nothings
            }
        }
    }

}
