package tinhn.kafka.training;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * java -cp kafka-java-example_2.0.0-1.0.jar tinhn.kafka.training.ProducerExample <brokers> <topics> <username> <password>
 */
public class ProducerExample {
    private static Logger logger = LoggerFactory.getLogger(ProducerExample.class);

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
        logger.info("Start example");

        Properties props = new MyKafkaConfig(username, password).GetKafkaProperties(brokers);
        //RUN_EXAMPLE01(props,topics);
        RUN_EXAMPLE02(props, topics);
    }

    public static void RUN_EXAMPLE01(Properties props, String topic) {
        Thread one = new Thread() {
            public void run() {
                try {
                    Producer<String, String> producer = new KafkaProducer<>(props);
                    int i = 0;
                    while (true) {
                        Date d = new Date();
                        producer.send(new ProducerRecord<>(topic, Integer.toString(i), d.toString()));
                        Thread.sleep(1000);
                        i++;
                    }
                } catch (InterruptedException ex) {
                    System.out.println(ex);
                }
            }
        };
        one.start();
    }

    public static void RUN_EXAMPLE02(Properties props, String topic) {
        Thread one = new Thread() {
            public void run() {
                List<String> carList = Arrays.asList("Ford", "Mazda", "KIA", "Toyota", "Mecedez-Ben");
                Random rnd1 = new Random();
                Random rnd2 = new Random();

                int rndStart = 0, rndEnd = 500;

                int n = rndStart + rnd2.nextInt(rndEnd - rndStart + 1);
                int loopStep = 1;
                int step = 0;

                try {
                    Producer<String, String> producer = new KafkaProducer<>(props);

                    while (true) {
                        for (int i = 0; i <= n; i++) {
                            SimpleDateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String strDate = dayFormat.format(Calendar.getInstance().getTime());
                            String uukey = UUID.randomUUID().toString().split("-")[0];

                            String strValue = strDate + "," + carList.get(rnd1.nextInt(carList.size()));

                            producer.send(new ProducerRecord<>(topic, uukey, strValue));
                            System.out.println("Send data: " + strValue);
                        }

                        try {
                            Thread.sleep(30 * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        step += 1;
                        if (loopStep > 0 && step == loopStep)
                            break;
                    }

                } catch (Exception ex) {
                    System.out.println(ex);
                }
            }
        };
        one.start();
    }
}
