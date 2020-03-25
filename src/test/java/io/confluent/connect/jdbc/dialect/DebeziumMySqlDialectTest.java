package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.debezium.DebeziumMySqlDialect;
import io.confluent.connect.jdbc.util.ColumnId;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DebeziumMySqlDialectTest extends BaseDialectTest<DebeziumMySqlDialect> {


    @Override
    protected DebeziumMySqlDialect createDialect() {
        return new DebeziumMySqlDialect(sinkConfigWithUrl("jdbc:mysql://something"));
    }

    @Test
    public void shouldBuildUpsertStatement() {
        String expected = "insert into `myTable`(`id1`,`id2`,`columnA`,`columnB`,`columnC`,`columnD`)" +
                " values(?,?,?,?,?,?) on duplicate key update `columnA`=values(`columnA`)," +
                "`columnB`=values(`columnB`),`columnC`=values(`columnC`),`columnD`=values" +
                "(`columnD`)";
        String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
        assertEquals(expected, sql);
    }

    @Test
    public void shouldBuildUpsertStatementWithColumnHasAliasCaseOne() {
        String expected = "insert into `myTable`(`sid`,`cid`,`newColumnA`,`newColumnB`,`newColumnC`,`newColumnD`)" +
                " values(?,?,?,?,?,?) on duplicate key update `newColumnA`=values(`newColumnA`)," +
                "`newColumnB`=values(`newColumnB`),`newColumnC`=values(`newColumnC`),`newColumnD`=values" +
                "(`newColumnD`)";
        columnPK1 = new ColumnId(tableId, "id1", "sid");
        columnPK2 = new ColumnId(tableId, "id2", "cid");
        columnA = new ColumnId(tableId, "columnA" ,"newColumnA");
        columnB = new ColumnId(tableId, "columnB", "newColumnB");
        columnC = new ColumnId(tableId, "columnC", "newColumnC");
        columnD = new ColumnId(tableId, "columnD", "newColumnD");
        pkColumns = Arrays.asList(columnPK1, columnPK2);
        columnsAtoD = Arrays.asList(columnA, columnB, columnC, columnD);
        String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
        assertEquals(expected, sql);
    }

    @Test
    public void shouldBuildUpsertStatementWithColumnHasAliasCaseTwo() {
        String expected = "insert into `myTable`(`sid`,`id2`,`newColumnA`,`columnB`,`newColumnC`,`columnD`)" +
                " values(?,?,?,?,?,?) on duplicate key update `newColumnA`=values(`newColumnA`)," +
                "`columnB`=values(`columnB`),`newColumnC`=values(`newColumnC`),`columnD`=values" +
                "(`columnD`)";
        columnPK1 = new ColumnId(tableId, "id1", "sid");
        columnPK2 = new ColumnId(tableId, "id2");
        columnA = new ColumnId(tableId, "columnA" ,"newColumnA");
        columnB = new ColumnId(tableId, "columnB");
        columnC = new ColumnId(tableId, "columnC", "newColumnC");
        columnD = new ColumnId(tableId, "columnD");
        pkColumns = Arrays.asList(columnPK1, columnPK2);
        columnsAtoD = Arrays.asList(columnA, columnB, columnC, columnD);
        String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
        assertEquals(expected, sql);
    }

}
