package io.confluent.developer;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerExample {

    private static Properties loadConfig() {
        Properties config = new Properties();
        try (InputStream input = ProducerExample.class
                .getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input != null) {
                config.load(input);
                System.out.println("loaded config.properties");
            } else {
                System.err.println("can't find config.properties");
            }
        } catch (Exception e) {
            System.err.println("failed to load config.properties: " + e.getMessage());
            e.printStackTrace();
        }
        return config;
    }

    public static void main(final String[] args) {
        Properties config = loadConfig();
        
        String kafkaKey = config.getProperty("kafka.key");
        String kafkaSecret = config.getProperty("kafka.secret");
        String bootstrapServers = config.getProperty("bootstrap.servers");
        String topicName = config.getProperty("topic.name", "purchases");
        
        if (kafkaKey == null || kafkaSecret == null || kafkaKey.equals("your-confluent-api-key")) {
            System.err.println("please set correct kafka.key and kafka.secret in config.properties");
            return;
        }

        String jaasConfig = String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", 
            kafkaKey, kafkaSecret);
        

        final Properties props = new Properties() {{
            put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            put(SASL_JAAS_CONFIG, jaasConfig);

            put(KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getCanonicalName());
            put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
            put(ACKS_CONFIG,                   "all");
            put(SECURITY_PROTOCOL_CONFIG,      "SASL_SSL");
            put(SASL_MECHANISM,                "PLAIN");
        }};

        String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"};
        String[] items = {"book", "alarm clock", "t-shirts", "gift card", "batteries"};
        
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final int numMessages = 10;
            System.out.println("start to send messages...");
            
            for (int i = 0; i < numMessages; i++) {
                String user = users[rnd.nextInt(users.length)];
                String item = items[rnd.nextInt(items.length)];

                producer.send(
                    new ProducerRecord<>(topicName, user, item),
                    (event, ex) -> {
                        if (ex != null) {
                            System.err.println("send failed: " + ex.getMessage());
                            ex.printStackTrace();
                        } else {
                            System.out.printf("message sent to topic %s: key = %-10s value = %s%n", topicName, user, item);
                        }
                    }
                );
                Thread.sleep(1000);
            }
            System.out.printf("sent %d messages to topic %s%n", numMessages, topicName);
        } catch (Exception e) {
            System.err.println("failed to create Kafka producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}