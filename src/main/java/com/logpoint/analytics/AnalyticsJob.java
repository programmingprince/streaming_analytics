package com.logpoint.analytics;

import com.immunesecurity.shared.exception.MappingException;
import com.immunesecurity.shared.json.JsonConverter;
import com.immunesecurity.shared.lib.Log;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class AnalyticsJob implements Runnable {

    private Properties streamsConfiguration;
    private StreamsBuilder streamsBuilder;
    private KStream<String, byte[]> kStream;
    private static final String INPUT_TOPIC = "analytics-input-topic";
    private static final String OUTPUT_TOPIC = "analytics-output-topic";

    //kafka declarations for producer
    private KafkaProducer<String, byte[]> kafkaProducer;
    private Properties configPropertiesForKafkaProducer;
    long windowSizeMs;


    public AnalyticsJob() {

        windowSizeMs = TimeUnit.MINUTES.toMillis(5);

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "client_id");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

        streamsBuilder = new StreamsBuilder();
        kStream = streamsBuilder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.ByteArray()));

        //kafka initialisations for producer
        configPropertiesForKafkaProducer = new Properties();
        configPropertiesForKafkaProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configPropertiesForKafkaProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configPropertiesForKafkaProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer<String, byte[]>(configPropertiesForKafkaProducer);
    }

    public void startAnalytics() {
        Topology topology = streamsBuilder.build();

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


        kStream.map((key, value) -> {
            try {
                return KeyValue.pair((String) ((Map) JsonConverter.byteToObject(value, HashMap.class)).get("device_ip"), value);
            } catch (MappingException e) {
                e.printStackTrace();
            }
            return null;
        }).groupByKey()
                .windowedBy(TimeWindows.of(windowSizeMs))
                .count(Materialized.as("testdfdf"))
                .toStream((key, value) -> {
                    Log.info("key: " + key + "key.key: " + key.key());
                    return key.key();
                })
                .map((key, value) -> {
                    Map<String, Object> event = new HashMap<String, Object>();
                    event.put("device_ip", key);
                    event.put("count", value);
                    event.put("time_stamp", new Date());
                    try {
                        Log.info("returning count for device " + key + " count: " + value);
                        return KeyValue.pair(key, JsonConverter.objectToByte(event));
                    } catch (MappingException e) {
                        e.printStackTrace();
                    }
                    return null;
                })
                .to(OUTPUT_TOPIC);


        startAnalytics();
        Random random = new Random();
        while (true) {
            Map<String, Object> eventMap = new HashMap<String, Object>();
            eventMap.put("device_ip", "192.168.1.1");
            ProducerRecord<String, byte[]> rec = null;
            try {
                rec = new ProducerRecord<String, byte[]>(INPUT_TOPIC, "192.168.1.1", JsonConverter.objectToByte(eventMap));
            } catch (MappingException e) {
                e.printStackTrace();
            }
            kafkaProducer.send(rec);

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}


