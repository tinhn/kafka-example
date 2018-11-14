package tinhn.kafka.training;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * java -cp kafka-java-producer_2.0.0-1.0.jar tinhn.kafka.training.ConsumerExample <brokers> <topics> <username> <password>
 */
public class ConsumerExample {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: ProducerExample <brokers> <topics> <username> <password>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n" +
                    "  <username> is an account for authentication with kafka cluster\n\n" +
                    "  <password> is a password of this account for authentication\n\n");
            System.exit(1);
        }

        String brokers = args[0];
        String topics = args[1];

        String username = args[2];
        String password = args[3];

        Properties props = new MyKafkaConfig(username, password).GetKafkaProperties(brokers);
        //RUN_CONSUM01(props,topics);
        RUN_CONSUM02(props,topics);
    }

    public static void RUN_CONSUM01(Properties props, String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("%s [%d] offset=%d, key=%s, value=\"%s\"\n",
                        record.topic(), record.partition(),
                        record.offset(), record.key(), record.value());
            }
        }
    }

    public static void RUN_CONSUM02(Properties props, String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        final int minBatchSize = 100;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {

                for (ConsumerRecord<String, String> record : records) {
                    String kafvalue = record.value();
                    System.out.println(kafvalue);
                }

                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}
