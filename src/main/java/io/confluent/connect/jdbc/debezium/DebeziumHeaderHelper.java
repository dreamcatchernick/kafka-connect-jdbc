package io.confluent.connect.jdbc.debezium;

import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

public class DebeziumHeaderHelper {

    public static String DEBEZIUM_OPERATION_KEY = "__debezium-operation";

    //return operation value c for create , u for update. else return empty string
    public static String getHeaderOperation(SinkRecord sinkRecord) {
        if(sinkRecord.headers().size() > 0) {
            for (Header header : sinkRecord.headers()) {
                if(header.key().equals(DEBEZIUM_OPERATION_KEY)) {
                    return header.value().toString();
                }
            }
            return "";
        }
        return "";
    }
}