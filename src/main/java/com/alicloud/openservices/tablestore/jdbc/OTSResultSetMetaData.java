package com.alicloud.openservices.tablestore.jdbc;


import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.sql.SQLColumnSchema;
import com.alicloud.openservices.tablestore.model.sql.SQLTableMeta;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class OTSResultSetMetaData extends WrapperAdapter implements ResultSetMetaData {

    private final List<String> columnNames;
    private final List<ColumnType> columnTypes;

    OTSResultSetMetaData(SQLTableMeta meta) {
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();
        List<SQLColumnSchema> columns = meta.getSchema();
        for (SQLColumnSchema column : columns) {
            columnNames.add(column.getName());
            columnTypes.add(column.getType());
        }
    }

    OTSResultSetMetaData(List<String> columnNames, List<ColumnType> columnTypes) {
        assert columnNames.size() == columnTypes.size();
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    private int toZeroIndex(int column) throws SQLException {
        assert columnNames.size() == columnTypes.size();
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("column index out of range");
        }
        return column - 1;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        ColumnType type = columnTypes.get(toZeroIndex(column));
        switch (type) {
            case BOOLEAN:
                return boolean.class.getName();
            case INTEGER:
                return long.class.getName();
            case DOUBLE:
                return double.class.getName();
            case STRING:
                return String.class.getName();
            case BINARY:
                return byte[].class.getName();
            case DATETIME:
                return java.sql.Timestamp.class.getName();
            case TIME:
                return java.sql.Time.class.getName();
            case DATE:
                return java.sql.Date.class.getName();
            default:
                return null;
        }
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        ColumnType type = columnTypes.get(toZeroIndex(column));
        switch (type) {
            case BOOLEAN:
                return 5;   // false
            case INTEGER:
                return 20;  // -9223372036854775807
            case DOUBLE:
                return 24;  // -2.2250738585072014E-308
            case STRING:
                return OTSDatabaseMetaData.MAX_STRING_LENGTH;
            case BINARY:
                return OTSDatabaseMetaData.MAX_BINARY_LENGTH;
            case DATETIME:
                return 19;  // 2019-01-01 00:00:00
            case TIME:
                return 8;   // 00:00:00
            case DATE:
                return 10;  // 2019-01-01
            default:
                return 0;
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columnNames.get(toZeroIndex(column));
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(toZeroIndex(column));
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        ColumnType type = columnTypes.get(toZeroIndex(column));
        switch (type) {
            case BOOLEAN:
                return Types.BOOLEAN;
            case INTEGER:
                return Types.BIGINT;
            case DOUBLE:
                return Types.DOUBLE;
            case STRING:
                return Types.VARCHAR;
            case BINARY:
                return Types.VARBINARY;
            case DATETIME:
                return Types.TIMESTAMP;
            case TIME:
                return Types.TIME;
            case DATE:
                return Types.DATE;
            default:
                return 0;
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        ColumnType type = columnTypes.get(toZeroIndex(column));
        switch (type) {
            case BOOLEAN:
                return "BOOL";
            case INTEGER:
                return "BIGINT";
            case DOUBLE:
                return "DOUBLE";
            case STRING:
                return "VARCHAR";
            case BINARY:
                return "VARBINARY";
            case DATETIME:
                return "TIMESTAMP";
            case TIME:
                return "TIME";
            case DATE:
                return "DATE";
            default:
                return null;
        }
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullableUnknown;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        ColumnType columnType = columnTypes.get(toZeroIndex(column));
        return columnType == ColumnType.DOUBLE || columnType == ColumnType.INTEGER;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }
}
