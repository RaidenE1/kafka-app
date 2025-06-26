package io.confluent.developer;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import org.apache.kafka.clients.consumer.Consumer;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerExample {

    private static Properties loadConfig() {
        Properties config = new Properties();
        try (InputStream input = ConsumerExample.class
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

            put(KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getCanonicalName());
            put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(GROUP_ID_CONFIG,                 "consumer-wordcount");
            put(AUTO_OFFSET_RESET_CONFIG,        "earliest");
            put(SECURITY_PROTOCOL_CONFIG,        "SASL_SSL");
            put(SASL_MECHANISM,                  "PLAIN");
        }};
        
        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topicName));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("received message from topic %s: key = %-10s value = %s%n", topicName, record.key(), record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("failed to create Kafka producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}