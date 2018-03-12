package com.logpoint.analytics;

import com.immunesecurity.shared.exception.MappingException;
import com.immunesecurity.shared.json.JsonConverter;
import com.immunesecurity.shared.lib.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class AnalyticsJobUsingProcessor implements Runnable {

    private Properties streamsConfiguration;
    private StreamsBuilder streamsBuilder;
    private KStream<String, byte[]> kStream;
    private static final String INPUT_TOPIC1 = "_logpoint_normlogs";
    private static final String INPUT_TOPIC2 = "default_normlogs";
    private static final String INPUT_TOPIC3 = "_LogPointAlerts_normlogs";
    private static final String INPUT_TOPIC4 = "nirajan_normlogs";

    private static final String OUTPUT_TOPIC = "analytics-output-topic";

    //kafka declarations for producer
    private KafkaProducer<String, byte[]> kafkaProducer;
    private Properties configPropertiesForKafkaProducer;

    public AnalyticsJobUsingProcessor() {
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "client_id");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, TimeUnit.SECONDS.toMillis(0));
//        streamsConfiguration.put(StreamsConfig.POLL_MS_CONFIG, 5000);

        streamsBuilder = new StreamsBuilder();
//        kStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.ByteArray()));

        //kafka initialisations for producer
        configPropertiesForKafkaProducer = new Properties();
        configPropertiesForKafkaProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configPropertiesForKafkaProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configPropertiesForKafkaProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer<String, byte[]>(configPropertiesForKafkaProducer);
    }

    public void startAnalytics() {
        Topology topology = streamsBuilder.build();

        StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("deviceCount"),
                Serdes.String(),
                Serdes.Long())
                .withLoggingDisabled();

        topology.addSource("SOURCE1", INPUT_TOPIC1)
                .addSource("SOURCE2", INPUT_TOPIC2)
                .addSource("SOURCE3", INPUT_TOPIC3)
                .addSource("SOURCE4", INPUT_TOPIC4)
                .addProcessor("PROCESS", () -> new TestProcessor(), "SOURCE1", "SOURCE2", "SOURCE3", "SOURCE4")
                .addStateStore(countStoreSupplier, "PROCESS")
                .addSink("SINK", OUTPUT_TOPIC, "PROCESS");

        Log.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
            }
        });

        try {
            streams.start();
        } catch (Throwable e) {
            System.exit(1);
        }
    }

    @Override
    public void run() {
        startAnalytics();
//        int i=0;
//        Random random = new Random();
//        while (true) {
//            Map<String, Object> eventMap = new HashMap<String, Object>();
//            eventMap.put("device_ip", "192.168.1." + random.nextInt(2));
//            ProducerRecord<String, byte[]> rec = null;
//            try {
//                rec = new ProducerRecord<String, byte[]>(INPUT_TOPIC, JsonConverter.objectToByte(eventMap));
//            } catch (MappingException e) {
//                e.printStackTrace();
//            }
//            kafkaProducer.send(rec);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            i++;
//        }
    }
}



