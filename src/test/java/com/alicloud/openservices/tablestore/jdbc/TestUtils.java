package com.alicloud.openservices.tablestore.jdbc;

import org.junit.Assert;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TestUtils {

    static void assertResultSet(ResultSet resultSet, String[] header, Object[][] rows) throws SQLException {
        // check meta
        ResultSetMetaData meta = resultSet.getMetaData();
        Assert.assertEquals(header.length, meta.getColumnCount());
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            Assert.assertEquals(header[i - 1], meta.getColumnName(i));
        }
        // check data
        int rowCount = 0;
        for (Object[] row : rows) {
            Assert.assertTrue(String.format("More rows are expected at row %d.", rowCount), resultSet.next());
            Assert.assertEquals(header.length, row.length);
            for (int i = 0; i < header.length; i++) {
                Object expectedValue = row[i];
                if (expectedValue instanceof String) {
                    Assert.assertEquals(String.format("Assert fail line %d col %d", rowCount, i), expectedValue, resultSet.getString(header[i]));
                } else if (expectedValue instanceof Integer) {
                    Assert.assertEquals(String.format("Assert fail line %d col %d", rowCount, i), expectedValue, resultSet.getInt(header[i]));
                } else if (expectedValue instanceof Short) {
                    Assert.assertEquals(String.format("Assert fail line %d col %d", rowCount, i), expectedValue, resultSet.getShort(header[i]));
                } else if (expectedValue instanceof Double) {
                    Assert.assertEquals(String.format("Assert fail line %d col %d", rowCount, i), expectedValue, resultSet.getDouble(header[i]));
                } else if (expectedValue instanceof Boolean) {
                    Assert.assertEquals(String.format("Assert fail line %d col %d", rowCount, i), expectedValue, resultSet.getBoolean(header[i]));
                } else if (expectedValue instanceof byte[]) {
                    Assert.assertArrayEquals(String.format("Assert fail line %d col %d", rowCount, i), (byte[]) expectedValue, resultSet.getBytes(header[i]));
                } else if (expectedValue == null) {
                    Assert.assertNull(String.format("Assert fail line %d col %d", rowCount, i), resultSet.getObject(header[i]));
                } else {
                    Assert.fail(String.format("Unknown data type `%s`.", expectedValue.getClass().getName()));
                }
            }
            rowCount++;
        }
        Assert.assertFalse("Expect end of rows.", resultSet.next());
    }
}
