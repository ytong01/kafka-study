package cn.cloudwalk.demo01.lower;

import java.util.Objects;

public class KafkaTopicPartitionInfo {

    public final String topic;
    public final int partitionID;

    public KafkaTopicPartitionInfo(String topic, int partitionID) {
        this.topic = topic;
        this.partitionID = partitionID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KafkaTopicPartitionInfo that = (KafkaTopicPartitionInfo) o;

        if (partitionID != that.partitionID) return false;
        return topic != null ? topic.equals(that.topic) : that.topic == null;
    }

    @Override
    public int hashCode() {

        return Objects.hash(topic, partitionID);
    }
}
