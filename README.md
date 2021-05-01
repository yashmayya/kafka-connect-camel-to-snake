# kafka-connect-camel-to-snake
A Kafka Connect Single Message Transform (SMT) to convert field values from camelCase to snake_case

This SMT supports converting fields in the record Key or Value from camel case to snake case. If the value is not in camel case, the value is left unchanged.

Properties:

|Name|Description|Type|Importance|
|---|---|---|---|
|`field.name`| Field name to convert | String| High |

Example configs:

```
transforms=camel
transforms.camel.type=com.github.yashmayya.kafka.connect.smt.CamelToSnake$Value
transforms.camel.field.name="name"
```

This would convert a record like: `{"id":0, "name":"anExampleFieldValue"}` to `{"id":0, "name":"an_example_field_value"}`

----------
### How to use this

- Run `mvn clean package` in the repo's root directory
- Copy the created jar from the /target directory to some directory, say `kafka-connect-camel-to-snake`, in your Connect worker's plugin path
- Create a connector using this transform in its properties!

----------
### To-Do:

- ~Add unit tests~
- Allow converting field names themselves. Example: convert `{"id":0, "fieldNameExample":"value"}` to `{"id":0, "field_name_example":"value"}`. This will require adding an additional property
