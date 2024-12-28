package com.ake3m.hazelcast.demos;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import net.datafaker.Faker;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

public class Writer {
    private static final Faker faker = new Faker();
    private static final Logger log = LoggerFactory.getLogger(Writer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:18081");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(KafkaAvroDeserializerConfig.KEY_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);
        props.put(KafkaAvroDeserializerConfig.VALUE_SUBJECT_NAME_STRATEGY, RecordNameStrategy.class);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {

            while (true) {
                var address = Address.newBuilder()
                        .setStreet(faker.address().streetAddress())
                        .setCity(faker.address().city())
                        .setState(faker.address().state())
                        .setZipCode(faker.address().zipCode())
                        .setCountry(faker.address().country())
                        .build();
                var user = User.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName(faker.name().fullName())
                        .setEmail(faker.internet().emailAddress())
                        .setAddress(address)
                        .build();

                producer.send(new ProducerRecord<>("user", user), (recordMetadata, e) -> {
                    if (e != null) {
                        log.error("Error sending record", e);
                    } else {
                        log.info("Record sent: {}", recordMetadata);
                    }
                });
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}