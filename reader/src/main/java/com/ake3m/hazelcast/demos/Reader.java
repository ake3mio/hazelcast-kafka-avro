package com.ake3m.hazelcast.demos;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reader {
    private static final Logger log = LoggerFactory.getLogger(Reader.class);

    public static void main(String[] args) {
        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient();

        IMap<String, Data> map = hazelcastInstance.getMap("userCache");

        map.values()
                .stream()
                .map(data -> {
                    try {
                        return deserialise(data);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).forEach(value -> log.info("{}", value));

        map.addEntryListener((EntryAddedListener<String, Data>) entryEvent -> {
            try {
                log.info("{}", deserialise(entryEvent.getValue()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, true);

    }

    public static GenericRecord deserialise(Data data) throws Exception {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(data.schema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(data.data(), null);
        var genericRecord = datumReader.read(null, decoder);
        return SpecificData.get().deepCopy(genericRecord.getSchema(), genericRecord);
    }
}