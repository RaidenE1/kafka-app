package io.confluent.developer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Demonstrates, using the low-level Processor API, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
 */
public class WordCount {

    private static final String INPUT_TOPIC = "produce";
    private static final String OUTPUT_TOPIC = "streams-wordcount-output";

    private static Properties loadConfig() {
        Properties config = new Properties();
        try (InputStream input = WordCount.class
                .getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input != null) {
                config.load(input);
            } else {
                throw new RuntimeException("config.properties not found");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return config;
    }

    static void createWordCountStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        final KTable<String, Long> counts = source
            .peek((key, value) -> {
                System.out.println("Consumed: key=" + key + ", value=" + value);
            })
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count();

        counts.toStream()
            .peek((word, count) -> {
                System.out.println("Produced: key=" + word + ", value=" + count);
            })
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public static class WordCountProcessor implements Processor<String, String, String, Long> {
        private KeyValueStore<String, Long> kvStore;
        private ProcessorContext<String, Long> context;
        private final String storeName = "Counts";

        @Override
        public void init(ProcessorContext<String, Long> context) {
            this.context = context;
            this.kvStore = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<String, String> record) {
            System.out.println("Consumed: key=" + record.key() + ", value=" + record.value());
            final String[] words = record.value().toLowerCase(Locale.getDefault()).split("\\W+");

            for (final String word : words) {
                if(word.isEmpty()) continue;

                Long oldCount = this.kvStore.get(word);
                if (oldCount == null) {
                    oldCount = 0L;
                }
                final Long newCount = oldCount + 1;
                this.kvStore.put(word, newCount);

                System.out.println("Produced: key=" + word + ", value=" + newCount);
                context.forward(new Record<>(word, newCount, record.timestamp()));
            }
        }

        @Override
        public void close() {
        }
    }

    static Topology createWordCountTopology() {
        final Topology topology = new Topology();
        final String sourceName = "Source";
        final String processorName = "Process";
        final String sinkName = "Sink";
        final String stateStoreName = "Counts";

        topology.addSource(sourceName, INPUT_TOPIC);
        topology.addProcessor(processorName, WordCountProcessor::new, sourceName);

        final StoreBuilder<KeyValueStore<String, Long>> countStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(stateStoreName),
                        Serdes.String(),
                        Serdes.Long());

        topology.addStateStore(countStoreBuilder, processorName);
        topology.addSink(sinkName, OUTPUT_TOPIC, Serdes.String().serializer(), Serdes.Long().serializer(), processorName);
        return topology;
    }

    public static void main(final String[] args) throws IOException {
        Properties config = loadConfig();

        String kafkaKey = config.getProperty("kafka.key");
        String kafkaSecret = config.getProperty("kafka.secret");
        String bootstrapServers = config.getProperty("bootstrap.servers");

        String jaasConfig = String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", 
            kafkaKey, kafkaSecret);
        

        final Properties props = new Properties() {{
            put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            put(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
            put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
            // Note: To re-run the demo, you need to use the offset reset tool:
            // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }};

        // final StreamsBuilder builder = new StreamsBuilder();
        // createWordCountStream(builder);
        // final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // To run the PAPI version, comment out the three lines above and uncomment the two lines below
        final Topology topology = createWordCountTopology();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        streams.setStateListener((newState, oldState) -> {
            System.out.println("Streams state changed: " + oldState + " -> " + newState);
        });

        streams.setUncaughtExceptionHandler((exception) -> {
            System.err.println("exception: " + exception.getMessage());
            exception.printStackTrace();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("Starting Kafka Streams application...");
            System.out.println("Waiting for messages from topic '" + INPUT_TOPIC + "'...");
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}