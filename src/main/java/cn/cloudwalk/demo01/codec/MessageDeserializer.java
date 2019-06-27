package cn.cloudwalk.demo01.codec;

import cn.cloudwalk.demo01.Account;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

public class MessageDeserializer implements Deserializer<Account> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Account deserialize(String s, byte[] bytes) {
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        try (ObjectInputStream inputStream = new ObjectInputStream(stream)) {

            return (Account) inputStream.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}
