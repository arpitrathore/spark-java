package org.arpit.spark.stream03.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.net.URL;

public class AvroUtil {

    public static StructType buildDataFrameSchemaFromSchemaRegistrySchema(String schemaRegistryUrl,
                                                                          String kafkaTopic,
                                                                          int schemaVersion) throws IOException {
        String avroSchemaUrl = schemaRegistryUrl + "/subjects/" + kafkaTopic + "-value/versions/" + schemaVersion + "/schema";
        ArrayNode schema = (ArrayNode) new ObjectMapper().readTree(new URL(avroSchemaUrl)).path("fields");
        return buildStructType(schema);
    }

    private static StructType buildStructType(ArrayNode schema) {
        StructType structType = new StructType();
        for (JsonNode jsonNode : schema) {
            JsonNode logicalType = jsonNode.findValue("logicalType");
            JsonNode typeNode = jsonNode.findValue("type");
            DataType fieldType = DataTypes.StringType;
            if (typeNode instanceof TextNode) {
                fieldType = getStructFieldDataType(typeNode.asText(), logicalType);
            } else if (typeNode instanceof ArrayNode) {
                if (typeNode.size() == 2 && typeNode.get(0).isTextual() && typeNode.get(1).isTextual()) {
                    if (typeNode.get(0).asText().equals("null")) {
                        fieldType = getStructFieldDataType(typeNode.get(1).asText(), logicalType);
                    } else if (typeNode.get(1).asText().equals("null")) {
                        fieldType = getStructFieldDataType(typeNode.get(0).asText(), logicalType);
                    }
                }
            }

            String name = jsonNode.findValue("name").asText();
            structType = structType.add(name, fieldType);
        }
        return structType;
    }

    private static DataType getStructFieldDataType(String datatype,
                                                   JsonNode logicalType) {
        switch (datatype) {
            case "long": {
                if (logicalType == null) {
                    return DataTypes.LongType;
                } else if (logicalType.asText().equals("timestamp-millis")
                        || logicalType.asText().equals("timestamp-micros")) {
                    return DataTypes.TimestampType;
                }
            }
            case "int":
                return DataTypes.IntegerType;
            case "boolean":
                return DataTypes.BooleanType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "bytes":
                return DataTypes.BinaryType;
            default:
                return DataTypes.StringType;
        }
    }
}
