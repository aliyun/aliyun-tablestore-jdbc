package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.ClientException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class OTSDriverTest {

    static TestEnvironment testEnvironment;

    @BeforeClass
    public static void setupTestEnvironment() throws ClassNotFoundException {
        Class.forName(OTSDriver.class.getName());
        testEnvironment = new TestEnvironment("OTSIntegrationTest");
        testEnvironment.setup();
    }

    @AfterClass
    public static void clearTestEnvironment() {
        testEnvironment.clear();
    }

    @Test
    public void testGetCatalogs() throws SQLException {
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), testEnvironment.getUser(), testEnvironment.getPassword());
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet resultSet = meta.getCatalogs();
        TestUtils.assertResultSet(resultSet, new String[]{"TABLE_CAT"}, new Object[][]{new Object[]{testEnvironment.getInstanceName()}});
    }

    @Test
    public void testGetTables() throws SQLException {
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), testEnvironment.getUser(), testEnvironment.getPassword());
        DatabaseMetaData meta = connection.getMetaData();
        // get tables without table name pattern
        ResultSet resultSet = meta.getTables(testEnvironment.getInstanceName(), null, null, null);
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"},
                new Object[][]{new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "TABLE", null, null, null, null, null, null}});
        // get tables with table name pattern and types
        resultSet = meta.getTables(testEnvironment.getInstanceName(), null, "OTS%Test", new String[]{"TABLE", "VIEW"});
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"},
                new Object[][]{new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "TABLE", null, null, null, null, null, null}});
        // table name pattern mismatch
        resultSet = meta.getTables(testEnvironment.getInstanceName(), null, "OTSTest", new String[]{"TABLE", "VIEW"});
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"},
                new Object[][]{});
        // types mismatch
        resultSet = meta.getTables(testEnvironment.getInstanceName(), null, "OTS%Test", new String[]{"VIEW"});
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"},
                new Object[][]{});
    }

    @Test
    public void testGetColumns() throws SQLException {
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), testEnvironment.getUser(), testEnvironment.getPassword());
        DatabaseMetaData meta = connection.getMetaData();
        // get columns without table name pattern and column name pattern
        ResultSet resultSet = meta.getColumns(testEnvironment.getInstanceName(), null, null, null);
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE", "REMARKS",
                        "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                        "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"},
                new Object[][]{
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk1", Types.BIGINT, "BIGINT", 20, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 1, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk2", Types.VARCHAR, "VARCHAR", 1024, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 2, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk3", Types.VARBINARY, "VARBINARY", 1024, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 3, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col1", Types.BIGINT, "BIGINT", 20, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 4, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col2", Types.DOUBLE, "DOUBLE", 22, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 5, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col3", Types.LONGVARCHAR, "MEDIUMTEXT", 2097152, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 6, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col4", Types.LONGVARBINARY, "MEDIUMBLOB", 2097152, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 7, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col5", Types.BOOLEAN, "BOOLEAN", 1, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 8, "YES", null, null, null, null, "NO", "NO"},
                });
        // get columns without column name pattern
        resultSet = meta.getColumns(testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), null);
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE", "REMARKS",
                        "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                        "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"},
                new Object[][]{
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk1", Types.BIGINT, "BIGINT", 20, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 1, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk2", Types.VARCHAR, "VARCHAR", 1024, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 2, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk3", Types.VARBINARY, "VARBINARY", 1024, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 3, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col1", Types.BIGINT, "BIGINT", 20, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 4, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col2", Types.DOUBLE, "DOUBLE", 22, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 5, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col3", Types.LONGVARCHAR, "MEDIUMTEXT", 2097152, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 6, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col4", Types.LONGVARBINARY, "MEDIUMBLOB", 2097152, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 7, "YES", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "col5", Types.BOOLEAN, "BOOLEAN", 1, null, null, 10, DatabaseMetaData.columnNullable, null, null, null, null, null, 8, "YES", null, null, null, null, "NO", "NO"},
                });
        // get columns with table name pattern and column name pattern
        resultSet = meta.getColumns(testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk%");
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE", "REMARKS",
                        "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                        "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"},
                new Object[][]{
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk1", Types.BIGINT, "BIGINT", 20, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 1, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk2", Types.VARCHAR, "VARCHAR", 1024, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 2, "NO", null, null, null, null, "NO", "NO"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk3", Types.VARBINARY, "VARBINARY", 1024, null, null, 10, DatabaseMetaData.columnNoNulls, null, null, null, null, null, 3, "NO", null, null, null, null, "NO", "NO"},
                });
        // table name pattern mismatch
        resultSet = meta.getColumns(testEnvironment.getInstanceName(), null, "ABC", "pk%");
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE", "REMARKS",
                        "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                        "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"},
                new Object[][]{});
    }

    @Test
    public void testGetPrimaryKeys() throws SQLException {
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), testEnvironment.getUser(), testEnvironment.getPassword());
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet resultSet = meta.getPrimaryKeys(testEnvironment.getInstanceName(), null, testEnvironment.getTableName());
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"},
                new Object[][]{
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk1", 1, "PRIMARY"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk2", 2, "PRIMARY"},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), "pk3", 3, "PRIMARY"},
                });
    }

    @Test
    public void testGetIndexInfo() throws SQLException {
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), testEnvironment.getUser(), testEnvironment.getPassword());
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet resultSet = meta.getIndexInfo(testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, true);
        TestUtils.assertResultSet(resultSet,
                new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                        "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"},
                new Object[][]{
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), false, null, "PRIMARY", DatabaseMetaData.tableIndexOther, 1, "pk1", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), false, null, "PRIMARY", DatabaseMetaData.tableIndexOther, 2, "pk2", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), false, null, "PRIMARY", DatabaseMetaData.tableIndexOther, 3, "pk3", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col1_index", DatabaseMetaData.tableIndexOther, 1, "col1", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col1_index", DatabaseMetaData.tableIndexOther, 2, "pk1", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col1_index", DatabaseMetaData.tableIndexOther, 3, "pk2", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col1_index", DatabaseMetaData.tableIndexOther, 4, "pk3", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col3_index", DatabaseMetaData.tableIndexOther, 1, "col3", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col3_index", DatabaseMetaData.tableIndexOther, 2, "pk1", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col3_index", DatabaseMetaData.tableIndexOther, 3, "pk2", "A", 0, 0, null},
                        new Object[]{testEnvironment.getInstanceName(), null, testEnvironment.getTableName(), true, null, "OTSIntegrationTest_col3_index", DatabaseMetaData.tableIndexOther, 4, "pk3", "A", 0, 0, null},
                });
    }

    @Test
    public void testStatement() throws SQLException {
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), testEnvironment.getUser(), testEnvironment.getPassword());
        Statement statement = connection.createStatement();
        // test SELECT via execute query
        ResultSet resultSet = statement.executeQuery("SELECT * FROM " + testEnvironment.getTableName());
        TestUtils.assertResultSet(resultSet,
                new String[]{"pk1", "pk2", "pk3", "col1", "col2", "col3", "col4", "col5"},
                new Object[][]{
                        new Object[]{1, "1", new byte[]{1}, 1, 1.0, "1", new byte[]{1}, true},
                        new Object[]{2, "2", new byte[]{2}, 2, 2.0, "2", new byte[]{2}, false},
                        new Object[]{3, "3", new byte[]{3}, 3, 3.0, "3", new byte[]{3}, true},
                });
        Assert.assertEquals(-1, statement.getUpdateCount());
        // test SELECT via execute
        Assert.assertTrue(statement.execute("SELECT * FROM " + testEnvironment.getTableName() + " ORDER BY pk1 DESC"));
        resultSet = statement.getResultSet();
        TestUtils.assertResultSet(resultSet,
                new String[]{"pk1", "pk2", "pk3", "col1", "col2", "col3", "col4", "col5"},
                new Object[][]{
                        new Object[]{3, "3", new byte[]{3}, 3, 3.0, "3", new byte[]{3}, true},
                        new Object[]{2, "2", new byte[]{2}, 2, 2.0, "2", new byte[]{2}, false},
                        new Object[]{1, "1", new byte[]{1}, 1, 1.0, "1", new byte[]{1}, true},
                });
        Assert.assertEquals(-1, statement.getUpdateCount());
        // test CREATE via execute update
        Assert.assertEquals(0, statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + testEnvironment.getTableName() + "(pk1 BIGINT PRIMARY KEY )"));
        Assert.assertNull(statement.getResultSet());
        Assert.assertEquals(0, statement.getUpdateCount());
        // test CREATE via execute
        Assert.assertFalse(statement.execute("CREATE TABLE IF NOT EXISTS " + testEnvironment.getTableName() + "(pk1 BIGINT PRIMARY KEY )"));
        Assert.assertNull(statement.getResultSet());
        Assert.assertEquals(0, statement.getUpdateCount());
    }

    @Test
    public void testPreparedStatement() throws SQLException {
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), testEnvironment.getUser(), testEnvironment.getPassword());
        // set integer
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + testEnvironment.getTableName() + " WHERE pk1 = ?");
        statement.setLong(1, 1);
        ResultSet resultSet = statement.executeQuery();
        TestUtils.assertResultSet(resultSet,
                new String[]{"pk1", "pk2", "pk3", "col1", "col2", "col3", "col4", "col5"},
                new Object[][]{new Object[]{1, "1", new byte[]{1}, 1, 1.0, "1", new byte[]{1}, true}});
        // set double
        statement = connection.prepareStatement("SELECT * FROM " + testEnvironment.getTableName() + " WHERE col2 = ?");
        statement.setDouble(1, 2);
        resultSet = statement.executeQuery();
        TestUtils.assertResultSet(resultSet,
                new String[]{"pk1", "pk2", "pk3", "col1", "col2", "col3", "col4", "col5"},
                new Object[][]{new Object[]{2, "2", new byte[]{2}, 2, 2.0, "2", new byte[]{2}, false}});
        // set string
        statement = connection.prepareStatement("SELECT * FROM " + testEnvironment.getTableName() + " WHERE pk2 = ?");
        statement.setString(1, "3");
        resultSet = statement.executeQuery();
        TestUtils.assertResultSet(resultSet,
                new String[]{"pk1", "pk2", "pk3", "col1", "col2", "col3", "col4", "col5"},
                new Object[][]{new Object[]{3, "3", new byte[]{3}, 3, 3.0, "3", new byte[]{3}, true}});
        // set bytes
        statement = connection.prepareStatement("SELECT * FROM " + testEnvironment.getTableName() + " WHERE pk3 = ?");
        statement.setBytes(1, new byte[]{1});
        resultSet = statement.executeQuery();
        TestUtils.assertResultSet(resultSet,
                new String[]{"pk1", "pk2", "pk3", "col1", "col2", "col3", "col4", "col5"},
                new Object[][]{new Object[]{1, "1", new byte[]{1}, 1, 1.0, "1", new byte[]{1}, true}});
        // set boolean
        statement = connection.prepareStatement("SELECT * FROM " + testEnvironment.getTableName() + " WHERE col5 = ?");
        statement.setBoolean(1, false);
        Assert.assertTrue(statement.execute());
        resultSet = statement.getResultSet();
        TestUtils.assertResultSet(resultSet,
                new String[]{"pk1", "pk2", "pk3", "col1", "col2", "col3", "col4", "col5"},
                new Object[][]{new Object[]{2, "2", new byte[]{2}, 2, 2.0, "2", new byte[]{2}, false}});
    }

    @Test
    public void testPropertyConfiguration() throws SQLException {
        // test getConnection
        Properties properties = new Properties();
        properties.setProperty("user", testEnvironment.getUser());
        properties.setProperty("password", testEnvironment.getPassword());
        properties.setProperty("syncClientWaitFutureTimeoutInMillis", "1");
        Connection connection = DriverManager.getConnection(testEnvironment.getURL(), properties);
        Statement statement = connection.createStatement();
        Assert.assertThrows(ClientException.class, () -> statement.executeQuery("SELECT * FROM " + testEnvironment.getTableName()));

        // test setClientInfo
        connection.setClientInfo("syncClientWaitFutureTimeoutInMillis", "1000");
        connection.createStatement().executeQuery("SELECT * FROM " + testEnvironment.getTableName());
        connection.close();
    }
}
