package com.github.yashmayya.kafka.connect.smt;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public abstract class CamelToSnake<R extends ConnectRecord<R>> implements Transformation<R> {
    static String FIELD_NAME = "field.name";
    private static final String PURPOSE = "convert field from camelCase to snake_case";
    static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELD_NAME, Type.STRING, Importance.HIGH, PURPOSE);
    private String fieldName;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_NAME);
    }

    @Override
    public R apply(R record) {
        if (actualSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(actualValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(fieldName, convertCamelToSnake((String) value.get(fieldName)));
        return newRecord(record, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(actualValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(fieldName, convertCamelToSnake((String) value.get(fieldName)));

        return newRecord(record, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    private String convertCamelToSnake(String input) {
        if (input.contains(" ") || input.length() == 0) {
            return input;
        }
        StringBuilder output = new StringBuilder();
        output.append(Character.toLowerCase(input.charAt(0)));
        for (int i=1; i<input.length(); i++) {
            if (Character.isUpperCase(input.charAt(i))) {
                output.append("_");
                output.append(Character.toLowerCase(input.charAt(i)));
            } else {
                output.append(input.charAt(i));
            }
        }
        return output.toString();
    }

    protected abstract Schema actualSchema(R record);
    protected abstract Object actualValue(R record);
    protected abstract R newRecord(R record, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends CamelToSnake<R> {

        @Override
        protected Schema actualSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object actualValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Object updatedKey) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedKey, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends CamelToSnake<R> {

        @Override
        protected Schema actualSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object actualValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }
}
