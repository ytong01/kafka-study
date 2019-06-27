package cn.cloudwalk.demo05.processor;

import cn.cloudwalk.demo05.ignite.NetworkSignalIgniteRepository;
import cn.cloudwalk.demo05.parser.NetworkDataParser;
import cn.cloudwalk.demo05.validation.NetworkSignalValidator;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.UnsupportedEncodingException;

public class NetworkDataKafkaProcessor implements Processor<byte[], byte[]> {

    private NetworkDataParser parser;
    private ProcessorContext context;
    private NetworkSignalIgniteRepository repository;

    @Override
    public void init(ProcessorContext processorContext) {

        this.context = processorContext;
        this.context.schedule(1000);
        this.parser = new NetworkDataParser(new NetworkSignalValidator());
        this.repository = new NetworkSignalIgniteRepository();
    }

    @Override
    public void process(byte[] key, byte[] value) {

        try {
            String json = new String(value, "utf-8");
            parser.parser(json).forEach(domain -> repository.save(domain));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void punctuate(long l) {

        context.commit();
    }

    @Override
    public void close() {

        repository.close();
    }
}
