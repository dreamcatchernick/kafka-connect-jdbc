package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by dreamcatchernick on 2019/3/4.
 */
@Ignore
public class DebeziumMySqlDialectTest extends BaseDialectTest<DebeziumMySqlDialect> {

    @Override
    protected DebeziumMySqlDialect createDialect() {
        return new DebeziumMySqlDialect(sourceConfigWithUrl("jdbc:mysql://something"));
    }

    @Test
    public void bindFieldPrimitiveValues() throws SQLException {
        int index = ThreadLocalRandom.current().nextInt();

        //Schema DateTimeSchema3 = new SchemaBuilder(Schema.Type.INT64).name("io.debezium.time.Timestamp");

        //verifyBindField(++index, DateTimeSchema3, 1551436161535L);

        //Schema DateTimeSchema6 = new SchemaBuilder(Schema.Type.INT64).name("io.debezium.time.MicroTimestamp");

        //verifyBindField(++index, DateTimeSchema6, 1554892173038000L);

        //Schema TimeSchema = new SchemaBuilder(Schema.Type.INT64).name("io.debezium.time.MicroTime");

        //verifyBindField(++index, TimeSchema, 36000000000L);

        Schema DateSchema = new SchemaBuilder(Schema.Type.INT32).name("io.debezium.time.Date");

        verifyBindField(++index, DateSchema, 17960);
    }
}
