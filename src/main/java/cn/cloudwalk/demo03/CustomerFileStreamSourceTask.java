package cn.cloudwalk.demo03;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CustomerFileStreamSourceTask extends SourceTask {

    private String filename;
    private InputStream stream;
    private BufferedReader reader;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;
    private Long streamOffset;

    public String version() {

        return new CustomerFileStreamSourceConnector().version();
    }

    public void start(Map<String, String> props) {

        filename = props.get(CustomerFileStreamSourceConnector.FILE_CONFIG);
        if (filename == null || filename.isEmpty()) {

            stream = System.in;
            reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        }
        topic = props.get(CustomerFileStreamSourceConnector.TOPIC_CONFIG);
    }

    public List<SourceRecord> poll() throws InterruptedException {

        if (stream == null) {
            try{
                stream = new FileInputStream(filename);
                Map<String, Object> offset = context.offsetStorageReader()
                        .offset(Collections.singletonMap("filename", this.filename));

                if (offset != null) {
                    Object lastRecordedOffset = offset.get("position");
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long)) {
                        throw new ConnectException("Offset position is incorrectly type");
                    }
                    Long skipLeft = (Long) lastRecordedOffset;

                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public void stop() {

    }
}
