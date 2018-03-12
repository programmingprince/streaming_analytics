package com.logpoint.analytics;

import com.immunesecurity.shared.exception.MappingException;
import com.immunesecurity.shared.json.JsonConverter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestProcessor implements Processor {

    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        long windowSize = TimeUnit.SECONDS.toMillis(20);

        this.context.schedule(windowSize, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
            @Override
            public void punctuate(long timestamp) {
                KeyValueIterator<String, Long> iter = kvStore.all();

                while (iter.hasNext()) {
                    KeyValue<String, Long> entry = iter.next();
                    Map<String, Object> eventMap = new HashMap<String, Object>();
                    eventMap.put("device_ip", entry.key);
                    eventMap.put("count", entry.value);
                    eventMap.put("time_stamp", new Date());
                    try {
                        context.forward(entry.key, JsonConverter.objectToByte(eventMap));
                        kvStore.put(entry.key, 0L);
                    } catch (MappingException e) {
                        e.printStackTrace();
                    }
                }

                iter.close();

                context.commit();
            }
        });

        kvStore = (KeyValueStore<String, Long>) context.getStateStore("deviceCount");
    }

    @Override
    public void process(Object key, Object value) {
        byte[] data = (byte[]) value;
        String deviceIp = "";
        try {
            deviceIp = (String) ((Map) JsonConverter.byteToObject(data, HashMap.class)).get("device_ip");
        } catch (MappingException e) {
            e.printStackTrace();
        }


        Long oldValue = kvStore.get(deviceIp);

        if (oldValue == null) {
            this.kvStore.put(deviceIp, 1L);
        } else {
            this.kvStore.put(deviceIp, oldValue + 1L);
        }
    }

    @Override
    public void punctuate(long timestamp) {
    }

    @Override
    public void close() {

    }
}