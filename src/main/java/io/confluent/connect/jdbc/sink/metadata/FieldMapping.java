package io.confluent.connect.jdbc.sink.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldMapping {
    private String topicName;

    private String tableName;

    private String mapping;

    public FieldMapping() {}

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getMapping() {
        return mapping;
    }

    public void setMapping(String mapping) {
        this.mapping = mapping;
    }

    public Map<String,String> getMappingDetail() {
        Map<String,String> mappingDetails =  new HashMap<>();
        List<String> mappings = Arrays.asList(mapping.split(","));
        mappings.forEach((mapping) -> {
            String originalFieldName = mapping.split("->")[0].trim();
            String targetColumnName = mapping.split("->")[1].trim();
            mappingDetails.put(originalFieldName , targetColumnName);
        });
        return mappingDetails;
    }

}
