package com.ake3m.hazelcast.demos;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Sinks;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class Pipeline {
    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
    private static final int MAX_JOB_CANCELLATION_ATTEMPTS = 5;

    public static void main(String[] args) {
        var config = new Config();
        config.setLiteMember(true);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config.getJetConfig().setEnabled(true);
        config.setMapConfigs(
                Map.of(
                        "userCache", new MapConfig("userCache").setTimeToLiveSeconds(200)
                )
        );
        var hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        var pipeline = com.hazelcast.jet.pipeline.Pipeline.create();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "redpanda:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pipeline");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://redpanda:8081");
        props.put(KafkaAvroDeserializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        props.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

        pipeline.readFrom(KafkaSources.<String, GenericRecord>kafka(props, "user"))
                .withNativeTimestamps(0)
                .writeTo(Sinks.mapWithUpdating("userCache",
                        stringGenericRecordEntry -> stringGenericRecordEntry.getValue().get("id").toString(),
                        (oldValue, entry) -> serialise(entry.getValue())
                ));


        var jobName = "kafka-to-cache";
        JobConfig cfg = new JobConfig().addPackage("io.confluent", "com.ake3m.hazelcast.demos").setName(jobName);
        var jet = hazelcastInstance.getJet();

        Optional.ofNullable(jet.getJob(jobName))
                .ifPresent(Pipeline::cancelJob);

        jet.newJob(pipeline, cfg);
    }

    private static void cancelJob(Job job) {
        try {
            job.cancel();
            var i = 0;
            while (!job.getStatus().isTerminal()) {
                if (i > MAX_JOB_CANCELLATION_ATTEMPTS) {
                    log.error("Could not cancel job {} in {} attempts. Shutting down now.", job.getName(), i);
                    System.exit(1);
                    break;
                }
                Thread.sleep(500);
                i++;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static Data serialise(GenericRecord record) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(record.getSchema());
        Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(record, encoder);
        encoder.flush();
        outputStream.close();
        return new Data(record.getSchema(), outputStream.toByteArray());
    }
}