package com.logpoint.analytics.tsanomaly.mergerutils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.Map.Entry;

import com.logpoint.analytics.tsanomaly.AnomalyEngine;
import com.logpoint.analytics.tsanomaly.delta.RobustApproxStats;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.immunesecurity.shared.lib.Log;
import com.logpoint.analytics.tsanomaly.model.MovingMedian;

import com.immunesecurity.shared.exception.MappingException;
import com.immunesecurity.shared.json.JsonConverter;

import com.logpoint.analytics.tsanomaly.AnomalyModel;
import com.logpoint.analytics.tsanomaly.delta.RunningStats;
import com.logpoint.analytics.tsanomaly.logger.SyslogTCP;
import com.logpoint.analytics.tsanomaly.model.PredictiveModel;

public class AnomalyWorkerThread implements Runnable {

    private SyslogTCP syslogTCP;

    //kafka declarations for consumer
    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private Properties configPropertiesForKafkaConsumer;

    private static final String INPUT_TOPIC = "analytics-output-topic";

    public AnomalyWorkerThread(SyslogTCP syslogTCP) {
        this.syslogTCP = syslogTCP;

        //kafka initialisations for consumer
        configPropertiesForKafkaConsumer = new Properties();
        configPropertiesForKafkaConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configPropertiesForKafkaConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configPropertiesForKafkaConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        configPropertiesForKafkaConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
        configPropertiesForKafkaConsumer.put(ConsumerConfig.CLIENT_ID_CONFIG, "client_id");
        kafkaConsumer = new KafkaConsumer<String, byte[]>(configPropertiesForKafkaConsumer);
        kafkaConsumer.subscribe(Arrays.asList(INPUT_TOPIC));
    }

    @Override
    public void run() {
        try {
            while (!Thread.interrupted()) {
                try {
                    ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(60);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        String key = record.key();
                        byte[] recvResult = record.value();
                        Map<String, Object> resultHashMap =
                                JsonConverter.byteToHashMap(recvResult);
                        detectAnomaly(key, resultHashMap);
                    }
                } catch (MappingException e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param key_: the key for persisting model in leveldb
     * @param row   row to be processed
     */
    private void detectAnomaly(String key_, Map<String, Object> row) {
        Object object = row.get("count");
        if (object instanceof String) {
            try {
                object = Double.valueOf((String) object);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        if (object instanceof Number) {

            AnomalyModel anomalyModel;
            if (AnomalyEngine.anomalyModelMap.containsKey(key_)) {
                anomalyModel = AnomalyEngine.anomalyModelMap.get(key_);
            } else {
                Log.info(
                        "AnomalyEngine; model does not exist; Creating a new model");
                PredictiveModel model = new MovingMedian();
                RunningStats delta = new RobustApproxStats();
                anomalyModel = new AnomalyModel(model, delta);
                AnomalyEngine.anomalyModelMap.put(key_, anomalyModel);
            }

            double anomaly_score =
                    anomalyModel.push(((Number) object).doubleValue());
            Double threshold = 5.0;
            Log.info("device: " + key_ + " count: " + ((Number) object).doubleValue() + " score: " + anomaly_score);
            if (anomaly_score > threshold) {
                Map<String, Object> deepClone =
                        (Map<String, Object>) deepClone(row);
                deepClone.put("source_device_ip",
                        deepClone.get("device_ip"));
                Iterator<Entry<String, Object>> iterator =
                        deepClone.entrySet().iterator();
                while (iterator.hasNext()) {
                    Entry<String, Object> next = iterator.next();
                    String rowKey = next.getKey();
                    if (rowKey.equals("device_ip")) {
                        iterator.remove();
                    }

                }
                deepClone.put("anomaly_field",
                        "device_ip");
                deepClone.put("anomaly_score", anomaly_score);
                Log.info("Sending log to tcp...");
                try {
                    String logToBeSent =
                            JsonConverter.hashMapToJson(deepClone);
                    syslogTCP.sendLog(logToBeSent.substring(1,
                            logToBeSent.length() - 1));
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    Log.warn(
                            "TsAnomaly, Error connecting to syslog server");
                } catch (MappingException e) {
                    // TODO Auto-generated catch block
                    Log.warn(
                            "TsAnomaly, Error converting to json format");
                }

            }
        } else {
            // LogUtil.warn.log("Encountered and skipping non numeric
            // value ", object);
            Log.warn("Encountered and skipping non numeric value");
        }
    }

    public Object deepClone(Object object) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            ByteArrayInputStream bais =
                    new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
