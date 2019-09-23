/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link DatabaseDialect} for Debezium to MySql.
 */
public class DebeziumMySqlDialect extends GenericDatabaseDialect {
    /**
     * The provider for {@link DebeziumMySqlDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(DebeziumMySqlDialect.class.getSimpleName(), "debeziummysql");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new DebeziumMySqlDialect(config);
        }
    }

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public DebeziumMySqlDialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "`", "`"));
    }

    /**
     * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
     * the {@link #createPreparedStatement(Connection, String)} method after the statement is
     * created but before it is returned/used.
     *
     * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
     * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
     * scroll more efficiently through the result set and prevent out of memory errors.
     *
     * @param stmt the prepared statement; never null
     * @throws SQLException the error that might result from initialization
     */
    @Override
    protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
        log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
    }


    @Override
    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                    // Maximum precision supported by MySQL is 65
                    int scale = Integer.parseInt(field.schemaParameters().get(Decimal.SCALE_FIELD));
                    return "DECIMAL(65," + scale + ")";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                    return "TIME(3)";
                case Timestamp.LOGICAL_NAME:
                    return "DATETIME(3)";
                default:
                    // pass through to primitive types
            }
        }
        switch (field.schemaType()) {
            case INT8:
                return "TINYINT";
            case INT16:
                return "SMALLINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "TINYINT";
            case STRING:
                return "VARCHAR(256)";
            case BYTES:
                return "VARBINARY(1024)";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildUpsertQueryStatement(
            TableId table,
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        //MySql doesn't support SQL 2003:merge so here how the upsert is handled
        final Transform<ColumnId> transform = (builder, col) -> {
            builder.appendColumnName(col.name());
            builder.append("=values(");
            builder.appendColumnName(col.name());
            builder.append(")");
        };

        ExpressionBuilder builder = expressionBuilder();
        builder.append("insert into ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") values(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(") on duplicate key update ");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(transform)
                .of(nonKeyColumns.isEmpty() ? keyColumns : nonKeyColumns);
        return builder.toString();
    }


    protected boolean maybeBindPrimitive(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
        //fix debezium extract data type - time/datatime to timestamp(long)
        if(schema.name() != null && schema.name().contains("io.debezium.time")) {
            long sourceDataTimezoneHours = ((JdbcSinkConfig) config).sourceDataTimezoneHours;
            switch (schema.name()) {
                case "io.debezium.time.ZonedTimestamp":
                    ZonedDateTime zonedDateTime = ZonedDateTime.parse((String)value, DateTimeFormatter.ISO_DATE_TIME);
                    statement.setTimestamp(
                            index,
                            java.sql.Timestamp.from(zonedDateTime.toInstant())
                    );
                    return true;
                case "io.debezium.time.Timestamp":
                case "io.debezium.time.MicroTimestamp":
                    Instant instant = Instant.ofEpochMilli((long)value);
                    Instant instantAtUTC = instant.minus(sourceDataTimezoneHours, ChronoUnit.HOURS);
                    statement.setTimestamp(
                            index,
                            java.sql.Timestamp.from(instantAtUTC)
                    );
                    return true;
                case "io.debezium.time.MicroTime":
                    Instant instantInSourceTimeZone = Instant.ofEpochMilli((long)value);
                    long instantInUTC = instantInSourceTimeZone.minus(sourceDataTimezoneHours, ChronoUnit.HOURS).toEpochMilli();
                    statement.setTime(
                            index,
                            new java.sql.Time(instantInUTC)
                    );
                    return true;
                case "io.debezium.time.Date":
                    LocalDate localDate = LocalDate.ofEpochDay(((Integer)value).longValue());
                    statement.setDate(
                            index,
                            java.sql.Date.valueOf(localDate)
                    );
                    return true;
                default:
                    break;
            }
        }

        return super.maybeBindPrimitive(statement, index, schema, value);
    }

    @Override
    protected String sanitizedUrl(String url) {
        // MySQL can also have "username:password@" at the beginning of the host list and
        // in parenthetical properties
        return super.sanitizedUrl(url)
                .replaceAll("(?i)([(,]password=)[^,)]*", "$1****")
                .replaceAll("(://[^:]*:)([^@]*)@", "$1****@");
    }

    @Override
    protected Set<ColumnId> primaryKeyColumns(
            Connection connection,
            String catalogPattern,
            String schemaPattern,
            String tablePattern
    ) throws SQLException {
        final Set<ColumnId> pkColumns = new HashSet<>();
        //here use schemaPattern as catalogPattern to fix mysql connector/j 8.0+ issue because it will threw issue if dont give catalogPattern
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(schemaPattern, schemaPattern, tablePattern)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                final String colName = rs.getString(4);
                ColumnId columnId = new ColumnId(tableId, colName);
                pkColumns.add(columnId);
            }
        }
        return pkColumns;
    }
}