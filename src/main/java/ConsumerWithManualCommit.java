import org.apache.kafka.clients.consumer.*;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class ConsumerWithManualCommit {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        // Mandatory properties
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "manual-group");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topics[] = {"numbers"};

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topics));

        final int minBatchSize = 200;
        String fileFullPath = "numbers.txt";
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        FileWriter fileWriter = new FileWriter(fileFullPath, true);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }
                if (buffer.size() >= minBatchSize) {
                    // Write to file
                    fileWriter.append(buffer.toString());
                    consumer.commitSync();
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            fileWriter.close();
        }
    }
}
