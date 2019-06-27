package cn.cloudwalk.demo01;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ProducerTest {

    public static void main(String[] args) throws IOException {

        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafka-produce.properties");
        Properties properties = new Properties();
        properties.load(stream);
        KafkaProducer producer = new KafkaProducer(properties);

        try {
            Account account = new Account();
            account.setId("rose");
            account.setPhone("13122701727");
            ProducerRecord<String, Account> producerRecord = new ProducerRecord<>("ip_login", account);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    System.out.println("消息发送成功");
                }
            });
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {

            producer.close();
        }
    }
}
