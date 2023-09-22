package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.model.sql.SQLResultSet;
import com.alicloud.openservices.tablestore.model.sql.SQLRow;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;


public class OTSResultSet extends WrapperAdapter implements ResultSet {

    private final OTSResultSetMetaData meta;
    private final Map<String, Integer> nameIndexMap;
    private List<Object[]> rows;
    private OTSStatement stmt;
    private int fetchSize;

    private boolean wasNull = false;
    private int rowIndex = 0;
    private SQLWarning warnings = null;
    private boolean isClosed = false;

    OTSResultSet(OTSStatement stmt, SQLResultSet resultSet, int maxRows) throws SQLException {
        this.stmt = stmt;
        this.meta = new OTSResultSetMetaData(resultSet.getSQLTableMeta());
        // build column map
        this.nameIndexMap = new HashMap<>();
        for (Map.Entry<String, Integer> column : resultSet.getSQLTableMeta().getColumnsMap().entrySet()) {
            nameIndexMap.put(column.getKey(), column.getValue() + 1);
        }
        // convert to objects
        this.rows = new ArrayList<>();
        while (resultSet.hasNext()) {
            SQLRow row = resultSet.next();
            Object[] objects = new Object[this.meta.getColumnCount()];
            for (int i = 0; i < this.meta.getColumnCount(); i++) {
                objects[i] = row.get(i);
            }
            rows.add(objects);
        }
        // truncate rows
        if (maxRows > 0 && this.rows.size() > maxRows) {
            this.rows = this.rows.subList(0, maxRows);
        }
    }

    OTSResultSet(OTSResultSetMetaData meta, List<Object[]> rows) throws SQLException {
        this.meta = meta;
        this.rows = rows;
        this.nameIndexMap = new HashMap<>();
        for (int i = 1; i <= this.meta.getColumnCount(); i++) {
            this.nameIndexMap.put(this.meta.getColumnName(i), i);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) {
        fetchSize = rows;
    }

    @Override
    public boolean absolute(int row) {
        if (row < 0) {
            rowIndex = rows.size() + 1 + row;
        } else {
            rowIndex = row;
        }
        return 0 < rowIndex && rowIndex <= rows.size();
    }

    @Override
    public void afterLast() {
        rowIndex = rows.size() + 1;
    }

    @Override
    public void beforeFirst() {
        rowIndex = 0;
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        warnings = null;
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean first() {
        if (rows.isEmpty()) {
            return false;
        }
        rowIndex = 1;
        return true;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getBinaryStream(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (value instanceof Boolean || value instanceof Long) {
            return new BigDecimal(getLong(columnIndex));
        } else if (value instanceof Double) {
            return BigDecimal.valueOf(getDouble(columnIndex));
        } else if (value instanceof String) {
            return new BigDecimal((String) value);
        } else if (value instanceof ByteBuffer) {
            return new BigDecimal(new String(unwrapByteBuffer((ByteBuffer) value)));
        } else {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), BigDecimal.class.getName()));
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBigDecimal(columnIndex);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (!nameIndexMap.containsKey(columnLabel)) {
            throw new SQLException(String.format("Column '%s' not found.", columnLabel));
        }
        return nameIndexMap.get(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        if (rowIndex < 1) {
            throw new SQLException("Before start of result set");
        } else if (rowIndex > rows.size()) {
            throw new SQLException("After end of result set");
        } else if (columnIndex < 1) {
            throw new SQLException(String.format("Column Index out of range, %d < 1.", columnIndex));
        } else if (columnIndex > meta.getColumnCount()) {
            throw new SQLException(String.format("Column Index out of range, %d > %d.", columnIndex, meta.getColumnCount()));
        }
        Object value = rows.get(rowIndex - 1)[columnIndex - 1];
        wasNull = value == null;
        return value;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getObject(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (!(value instanceof ByteBuffer)
                && !(value instanceof String)
                && !(value instanceof ZonedDateTime)
                && !(value instanceof Duration)
                && !(value instanceof LocalDate)) {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), InputStream.class.getName()));
        }
        return new ByteArrayInputStream(getBytes(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBinaryStream(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (!(value instanceof ByteBuffer)
                && !(value instanceof String)
                && !(value instanceof ZonedDateTime)
                && !(value instanceof Duration)
                && !(value instanceof LocalDate)) {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Blob.class.getName()));
        }
        return new SerialBlob(getBytes(columnIndex));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBlob(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return false;
        } else if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Long) {
            return (Long) value != 0;
        } else if (value instanceof Double) {
            return (Double) value != 0;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else if (value instanceof ByteBuffer) {
            return Boolean.parseBoolean(new String(unwrapByteBuffer((ByteBuffer) value)));
        } else {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Boolean.class.getName()));
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        long value = getLong(columnIndex);
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new SQLException(String.format("Value '%s' is outside of valid range for type %s",
                    getString(columnIndex), Byte.class.getName()));
        }
        return (byte) value;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getByte(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (value instanceof ByteBuffer) {
            return unwrapByteBuffer((ByteBuffer) value);
        } else if (value instanceof ZonedDateTime) {
            return Timestamp.valueOf(((ZonedDateTime) value).toLocalDateTime()).toString().getBytes();
        } else if (value instanceof Duration) {
            return String.valueOf(LocalTime.MIDNIGHT.plus((Duration) value)).getBytes();
        } else if (value instanceof Long
                || value instanceof Double
                || value instanceof Boolean
                || value instanceof String
                || value instanceof LocalDate) {
            return value.toString().getBytes();
        } else {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), byte[].class.getName()));
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getBytes(columnIndex);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (!(value instanceof ByteBuffer)
                && !(value instanceof String)
                && !(value instanceof ZonedDateTime)
                && !(value instanceof Duration)
                && !(value instanceof LocalDate)) {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Reader.class.getName()));
        }
        return new CharArrayReader(getString(columnIndex).toCharArray());
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getCharacterStream(columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (!(value instanceof ByteBuffer)
                && !(value instanceof String)
                && !(value instanceof ZonedDateTime)
                && !(value instanceof Duration)
                && !(value instanceof LocalDate)) {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Clob.class.getName()));
        }
        return new SerialClob(getString(columnIndex).toCharArray());
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getClob(columnIndex);
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return java.sql.Date.valueOf((String) value);
        } else if (value instanceof LocalDate) {
            return java.sql.Date.valueOf((LocalDate) value);
        }
        throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), java.sql.Date.class.getName()));
    }

    @Override
    public java.sql.Date getDate(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDate(columnIndex);
    }

    @Override
    public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public java.sql.Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return 0;
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Long) {
            return (double) (Long) value;
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1.0 : 0.0;
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        } else if (value instanceof ByteBuffer) {
            return Double.parseDouble(new String(unwrapByteBuffer((ByteBuffer) value)));
        } else {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Double.class.getName()));
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getDouble(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        double value = getDouble(columnIndex);
        if (Math.abs(value) > Float.MAX_VALUE) {
            throw new SQLException(String.format("Value '%s' is outside of valid range for type %s",
                    getString(columnIndex), Float.class.getName()));
        }
        return (float) value;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getFloat(columnIndex);
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        long value = getLong(columnIndex);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new SQLException(String.format("Value '%s' is outside of valid range for type %s",
                    getString(columnIndex), Integer.class.getName()));
        }
        return (int) value;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return 0;
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Double) {
            return ((Double) value).longValue();
        } else if (value instanceof Boolean) {
            return (Boolean) value ? 1L : 0L;
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        } else if (value instanceof ByteBuffer) {
            return Long.parseLong(new String(unwrapByteBuffer((ByteBuffer) value)));
        } else {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Long.class.getName()));
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getLong(columnIndex);
    }

    @Override
    public OTSResultSetMetaData getMetaData() {
        return meta;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return (String) value;
        } else if (value instanceof ByteBuffer) {
            return new String(unwrapByteBuffer((ByteBuffer) value));
        } else if (value instanceof ZonedDateTime) {
            return Timestamp.valueOf(((ZonedDateTime) value).toLocalDateTime()).toString();
        } else if (value instanceof Duration) {
            return String.valueOf(LocalTime.MIDNIGHT.plus((Duration) value));
        } else if (value instanceof Long
                || value instanceof Double
                || value instanceof Boolean
                || value instanceof LocalDate) {
            return value.toString();
        } else {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), String.class.getName()));
        }
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getString(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() {
        return rowIndex;
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        long value = getLong(columnIndex);
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new SQLException(String.format("Value '%s' is outside of valid range for type %s",
                    getString(columnIndex), Short.class.getName()));
        }
        return (short) value;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getShort(columnIndex);
    }

    @Override
    public OTSStatement getStatement() throws SQLException {
        checkClosed();
        return stmt;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return Time.valueOf(LocalTime.parse((String) value));
        } else if (value instanceof Duration) {
            return Time.valueOf(LocalTime.MIDNIGHT.plus((Duration) value));
        }
        throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Time.class.getName()));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            return Timestamp.valueOf((String) value);
        } else if (value instanceof ZonedDateTime) {
            return Timestamp.valueOf(((ZonedDateTime) value).toLocalDateTime());
        }
        throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), Timestamp.class.getName()));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        } else if (!(value instanceof ByteBuffer) && !(value instanceof String)) {
            throw new SQLException(String.format("Unsupported conversion from %s to %s", meta.getColumnTypeName(columnIndex), URL.class.getName()));
        }
        try {
            return new URL(getString(columnIndex));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        int columnIndex = findColumn(columnLabel);
        return getURL(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return warnings;
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public boolean isFirst() {
        return rowIndex == 1;
    }

    @Override
    public boolean isLast() {
        return rowIndex == rows.size();
    }

    @Override
    public boolean last() {
        if (rows.isEmpty()) {
            return false;
        }
        rowIndex = rows.size();
        return true;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, java.sql.Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (rowIndex < rows.size()) {
            rowIndex++;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("the result set has already been closed");
        }
    }

    private byte[] unwrapByteBuffer(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
