package com.alicloud.openservices.tablestore.jdbc;


import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.*;
import java.util.*;

public class OTSPreparedStatement extends OTSStatement implements PreparedStatement {

    private final String sql;
    private final List<Integer> placeholders;
    private final HashMap<Integer, Object> parameters;

    OTSPreparedStatement(OTSConnection conn, String sql) throws SQLException {
        super(conn);
        this.sql = sql;
        this.placeholders = findPlaceholders(sql);
        this.parameters = new HashMap<>();
    }

    static List<Integer> findPlaceholders(String sql) throws SQLException {
        if (sql == null) {
            throw new SQLException("SQL String cannot be NULL");
        }
        List<Integer> placeholders = new ArrayList<Integer>();
        char quoted = '\0';
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            switch (c) {
                case '\\':
                    i++;
                    break;
                case '\"':
                case '\'':
                case '`':
                    if (c == quoted) {
                        quoted = '\0';
                    } else {
                        quoted = c;
                    }
                    break;
                case '?':
                    if (quoted == '\0') {
                        placeholders.add(i);
                    }
                    break;
            }
        }
        return placeholders;
    }

    String interpolateParameters() throws SQLException {
        StringBuilder builder = new StringBuilder();
        int beginIndex = 0;
        for (int i = 0; i < placeholders.size(); i++) {
            int placeholder = placeholders.get(i);
            builder.append(sql, beginIndex, placeholder);
            Object value = parameters.get(i);
            builder.append(toSqlString(value));
            beginIndex = placeholder + 1;
        }
        builder.append(sql.substring(beginIndex));
        return builder.toString();
    }

    private String toSqlString(Object x) throws SQLException {
        if (x == null) {
            return "NULL";
        } else if (x instanceof Boolean
                || x instanceof Byte
                || x instanceof Short
                || x instanceof Integer
                || x instanceof Long
                || x instanceof Float
                || x instanceof Double) {
            return x.toString();
        } else if (x instanceof BigDecimal) {
            return ((BigDecimal) x).toPlainString();
        } else if (x instanceof String) {
            return StringUtils.quoteIdentifier(x.toString(), OTSDatabaseMetaData.quoteString);
        } else if (x instanceof byte[]) {
            return String.format("x'%s'", Hex.encodeHexString((byte[]) x));
        } else {
            throw new SQLException("unrecognized Java class: " + x.getClass().getName());
        }
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        parameters.clear();
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(interpolateParameters());
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(interpolateParameters());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(interpolateParameters());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return super.getResultSet().getMetaData();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else {
            try {
                setBytes(parameterIndex, IOUtils.toByteArray(x));
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else {
            try {
                setBytes(parameterIndex, IOUtils.toByteArray(x, length));
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else {
            setBinaryStream(parameterIndex, x.getBinaryStream());
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBinaryStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        if (reader == null) {
            setNull(parameterIndex, Types.NULL);
        } else {
            try {
                setString(parameterIndex, IOUtils.toString(reader));
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        if (reader == null) {
            setNull(parameterIndex, Types.NULL);
        } else {
            char[] buf = new char[length];
            try {
                int readLength = reader.read(buf);
                if (readLength != -1) {
                    buf = Arrays.copyOfRange(buf, 0, readLength);
                } else {
                    buf = new char[]{};
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
            setString(parameterIndex, new String(buf));
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int) length);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, Types.NULL);
        } else {
            setCharacterStream(parameterIndex, x.getCharacterStream());
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        if (value == null) {
            setNull(parameterIndex, Types.NULL);
        } else {
            setCharacterStream(parameterIndex, value.getCharacterStream());
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setObject(parameterIndex, null);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkClosed();
        // check parameter index
        if (parameterIndex < 1 || parameterIndex > placeholders.size()) {
            throw new SQLException("parameter index out of range");
        }
        // check parameter type
        if (x == null
                || x instanceof Boolean
                || x instanceof String
                || x instanceof Byte
                || x instanceof Short
                || x instanceof Integer
                || x instanceof Float
                || x instanceof Double
                || x instanceof Long
                || x instanceof BigDecimal
                || x instanceof byte[]) {
            parameters.put(parameterIndex - 1, x);
        } else if (x instanceof Reader) {
            setCharacterStream(parameterIndex, (Reader) x);
        } else if (x instanceof Clob) {
            setClob(parameterIndex, (Clob) x);
        } else if (x instanceof InputStream) {
            setBinaryStream(parameterIndex, (InputStream) x);
        } else if (x instanceof Blob) {
            setBlob(parameterIndex, (Blob) x);
        } else if (x instanceof URL) {
            setURL(parameterIndex, (URL) x);
        } else {
            throw new SQLException("unsupported type: " + x.getClass().getName());
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setLong(parameterIndex, x);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setDouble(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setLong(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setLong(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setString(parameterIndex, x.toString());
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, length);
    }
}
