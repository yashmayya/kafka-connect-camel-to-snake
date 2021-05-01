package com.github.yashmayya.kafka.connect.smt;

import static org.junit.Assert.assertEquals;

import com.github.yashmayya.kafka.connect.smt.CamelToSnake.Value;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CameToSnakeTest {
    private final CamelToSnake<SourceRecord> transform = new Value<>();

    @Before
    public void setUp() {
        final Map<String, Object> props = new HashMap<>();
        props.put(CamelToSnake.FIELD_NAME, "name");
        transform.configure(props);
    }

    @After
    public void tearDown() {
        transform.close();
    }

    @Test
    public void camelToSnakeSchemaless() {
        final SourceRecord record = new SourceRecord(null, null, "topic", null,
                Collections.singletonMap("name", "testCamelCaseString"));
        final SourceRecord transformedRecord = transform.apply(record);
        assertEquals(((Map) transformedRecord.value()).get("name"), "test_camel_case_string");
    }

    @Test
    public void camelToSnakeSchema() {
        final Schema structSchema = SchemaBuilder.struct().name("testSchema").field("name", Schema.STRING_SCHEMA).build();
        final Struct struct = new Struct(structSchema).put("name", "testCamelCaseString");
        final SourceRecord record = new SourceRecord(null, null, "topic", structSchema, struct);
        final SourceRecord transformedRecord = transform.apply(record);
        assertEquals(((Struct) transformedRecord.value()).getString("name"), "test_camel_case_string");
    }
}
