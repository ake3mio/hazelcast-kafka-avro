package com.ake3m.hazelcast.demos;

import org.apache.avro.Schema;

import java.io.Serial;
import java.io.Serializable;

public record Data(Schema schema, byte[] data) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
