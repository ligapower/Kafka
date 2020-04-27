import org.apache.kafka.clients.producer.*;

import java.util.Date;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        String clientId = "liga-producer";

        Properties props = new Properties();
        // Mandatory properties
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // Optional properties
        props.put("acks", "all");
        props.put("client.id", clientId);     // To identify the producer
        props.put("compression.type", "gzip");  //Compression method
        props.put("delivery.timeout.ms", 120000);

        KafkaProducer<String, String> producer = new KafkaProducer(props);

        int numOfRecords = 100;
        String topic = "numbers";

        // EXAMPLE 1 - Numbers as strings for key and value without any delay
        for (int i = 0; i < numOfRecords; i++) {
            System.out.println("Message " + i + " was just sent");
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
        }
        producer.close();

        /* EXAMPLE 2 - Formatted string as message and messages are sent with 333ms delay (3 messages / second)
        try {
            for (int i = 0; i < numOfRecords; i++) {
                String message = String.format("Producer %s has sent message %s at %s", clientId, i, new Date());
                System.out.println(message);
                producer.send(new ProducerRecord<>(topic, Integer.toString(i), message));
                Thread.sleep(300);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
        */
    }
}
