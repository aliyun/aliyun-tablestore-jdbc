package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.model.ColumnType;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class OTSResultSetTest {

    static final double EPSILON = 1e-10;

    @BeforeClass
    public static void loadDriver() throws ClassNotFoundException {
        Class.forName(OTSDriver.class.getName());
    }

    private static String readString(Reader reader) throws IOException {
        StringBuilder s = new StringBuilder();
        while (true) {
            char[] buf = new char[1024 * 1024];
            int length = reader.read(buf);
            if (length == -1) {
                break;
            }
            buf = Arrays.copyOfRange(buf, 0, length);
            s.append(buf);
        }
        return s.toString();
    }

    @Test
    public void testGetLong() throws SQLException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("LONG"), Collections.singletonList(ColumnType.INTEGER)),
                Collections.singletonList(new Object[]{6L}));
        Assert.assertTrue(resultSet.next());

        // test long
        Assert.assertEquals(6, resultSet.getByte(1));
        Assert.assertEquals(6, resultSet.getByte("LONG"));
        Assert.assertEquals(6, resultSet.getShort(1));
        Assert.assertEquals(6, resultSet.getShort("LONG"));
        Assert.assertEquals(6, resultSet.getInt(1));
        Assert.assertEquals(6, resultSet.getInt("LONG"));
        Assert.assertEquals(6, resultSet.getLong(1));
        Assert.assertEquals(6, resultSet.getLong("LONG"));
        Assert.assertEquals(new BigDecimal(6), resultSet.getBigDecimal(1));
        Assert.assertEquals(new BigDecimal(6), resultSet.getBigDecimal("LONG"));

        // test double
        Assert.assertEquals(6f, resultSet.getFloat(1), (float) EPSILON);
        Assert.assertEquals(6f, resultSet.getFloat("LONG"), (float) EPSILON);
        Assert.assertEquals(6, resultSet.getDouble(1), EPSILON);
        Assert.assertEquals(6, resultSet.getDouble("LONG"), EPSILON);

        // test string
        Assert.assertEquals("6", resultSet.getString(1));
        Assert.assertEquals("6", resultSet.getString("LONG"));
        Assert.assertEquals("6", resultSet.getNString(1));
        Assert.assertEquals("6", resultSet.getNString("LONG"));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getCharacterStream(1)));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getCharacterStream("LONG")));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getNCharacterStream(1)));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getNCharacterStream("LONG")));

        // test bytes
        Assert.assertArrayEquals("6".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("6".getBytes(), resultSet.getBytes("LONG"));

        // test boolean
        Assert.assertTrue(resultSet.getBoolean(1));
        Assert.assertTrue(resultSet.getBoolean("LONG"));
    }

    @Test
    public void testGetDouble() throws SQLException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("DOUBLE"), Collections.singletonList(ColumnType.DOUBLE)),
                Collections.singletonList(new Object[]{3.14}));
        Assert.assertTrue(resultSet.next());

        // test long
        Assert.assertEquals(3, resultSet.getByte(1));
        Assert.assertEquals(3, resultSet.getByte("DOUBLE"));
        Assert.assertEquals(3, resultSet.getShort(1));
        Assert.assertEquals(3, resultSet.getShort("DOUBLE"));
        Assert.assertEquals(3, resultSet.getInt(1));
        Assert.assertEquals(3, resultSet.getInt("DOUBLE"));
        Assert.assertEquals(3, resultSet.getLong(1));
        Assert.assertEquals(3, resultSet.getLong("DOUBLE"));
        Assert.assertEquals(new BigDecimal("3.14"), resultSet.getBigDecimal(1));
        Assert.assertEquals(new BigDecimal("3.14"), resultSet.getBigDecimal("DOUBLE"));

        // test double
        Assert.assertEquals(3.14f, resultSet.getFloat(1), (float) EPSILON);
        Assert.assertEquals(3.14f, resultSet.getFloat("DOUBLE"), (float) EPSILON);
        Assert.assertEquals(3.14, resultSet.getDouble(1), EPSILON);
        Assert.assertEquals(3.14, resultSet.getDouble("DOUBLE"), EPSILON);

        // test string
        Assert.assertEquals("3.14", resultSet.getString(1));
        Assert.assertEquals("3.14", resultSet.getString("DOUBLE"));
        Assert.assertEquals("3.14", resultSet.getNString(1));
        Assert.assertEquals("3.14", resultSet.getNString("DOUBLE"));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getCharacterStream(1)));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getCharacterStream("DOUBLE")));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getNCharacterStream(1)));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getNCharacterStream("DOUBLE")));

        // test bytes
        Assert.assertArrayEquals("3.14".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("3.14".getBytes(), resultSet.getBytes("DOUBLE"));

        // test boolean
        Assert.assertTrue(resultSet.getBoolean(1));
        Assert.assertTrue(resultSet.getBoolean("DOUBLE"));
    }

    @Test
    public void testGetBoolean() throws SQLException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("BOOLEAN"), Collections.singletonList(ColumnType.BOOLEAN)),
                Collections.singletonList(new Object[]{true}));
        Assert.assertTrue(resultSet.next());

        // test long
        Assert.assertEquals(1, resultSet.getByte(1));
        Assert.assertEquals(1, resultSet.getByte("BOOLEAN"));
        Assert.assertEquals(1, resultSet.getShort(1));
        Assert.assertEquals(1, resultSet.getShort("BOOLEAN"));
        Assert.assertEquals(1, resultSet.getInt(1));
        Assert.assertEquals(1, resultSet.getInt("BOOLEAN"));
        Assert.assertEquals(1, resultSet.getLong(1));
        Assert.assertEquals(1, resultSet.getLong("BOOLEAN"));
        Assert.assertEquals(new BigDecimal(1), resultSet.getBigDecimal(1));
        Assert.assertEquals(new BigDecimal(1), resultSet.getBigDecimal("BOOLEAN"));

        // test double
        Assert.assertEquals(1f, resultSet.getFloat(1), (float) EPSILON);
        Assert.assertEquals(1f, resultSet.getFloat("BOOLEAN"), (float) EPSILON);
        Assert.assertEquals(1, resultSet.getDouble(1), EPSILON);
        Assert.assertEquals(1, resultSet.getDouble("BOOLEAN"), EPSILON);

        // test string
        Assert.assertEquals("true", resultSet.getString(1));
        Assert.assertEquals("true", resultSet.getString("BOOLEAN"));
        Assert.assertEquals("true", resultSet.getNString(1));
        Assert.assertEquals("true", resultSet.getNString("BOOLEAN"));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getCharacterStream(1)));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getCharacterStream("BOOLEAN")));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getNCharacterStream(1)));
        Assert.assertThrows(SQLException.class, () -> readString(resultSet.getNCharacterStream("BOOLEAN")));

        // test bytes
        Assert.assertArrayEquals("true".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("true".getBytes(), resultSet.getBytes("BOOLEAN"));

        // test boolean
        Assert.assertTrue(resultSet.getBoolean(1));
        Assert.assertTrue(resultSet.getBoolean("BOOLEAN"));
    }

    @Test
    public void testGetString() throws SQLException, IOException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("STRING"), Collections.singletonList(ColumnType.STRING)),
                Collections.singletonList(new Object[]{"1688"}));
        Assert.assertTrue(resultSet.next());

        // test long
        Assert.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assert.assertThrows(SQLException.class, () -> resultSet.getByte("STRING"));
        Assert.assertEquals(1688, resultSet.getShort(1));
        Assert.assertEquals(1688, resultSet.getShort("STRING"));
        Assert.assertEquals(1688, resultSet.getInt(1));
        Assert.assertEquals(1688, resultSet.getInt("STRING"));
        Assert.assertEquals(1688, resultSet.getLong(1));
        Assert.assertEquals(1688, resultSet.getLong("STRING"));
        Assert.assertEquals(new BigDecimal(1688), resultSet.getBigDecimal(1));
        Assert.assertEquals(new BigDecimal(1688), resultSet.getBigDecimal("STRING"));

        // test double
        Assert.assertEquals(1688f, resultSet.getFloat(1), (float) EPSILON);
        Assert.assertEquals(1688f, resultSet.getFloat("STRING"), (float) EPSILON);
        Assert.assertEquals(1688, resultSet.getDouble(1), EPSILON);
        Assert.assertEquals(1688, resultSet.getDouble("STRING"), EPSILON);

        // test string
        Assert.assertEquals("1688", resultSet.getString(1));
        Assert.assertEquals("1688", resultSet.getString("STRING"));
        Assert.assertEquals("1688", resultSet.getNString(1));
        Assert.assertEquals("1688", resultSet.getNString("STRING"));
        Assert.assertEquals("1688", readString(resultSet.getCharacterStream(1)));
        Assert.assertEquals("1688", readString(resultSet.getCharacterStream("STRING")));
        Assert.assertEquals("1688", readString(resultSet.getNCharacterStream(1)));
        Assert.assertEquals("1688", readString(resultSet.getNCharacterStream("STRING")));
        Assert.assertEquals("1688", readString(resultSet.getClob(1).getCharacterStream()));
        Assert.assertEquals("1688", readString(resultSet.getClob("STRING").getCharacterStream()));

        // test bytes
        Assert.assertArrayEquals("1688".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("1688".getBytes(), resultSet.getBytes("STRING"));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream(1)));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream("STRING")));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream(1)));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream("STRING")));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBlob(1).getBinaryStream()));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBlob("STRING").getBinaryStream()));

        // test boolean
        Assert.assertFalse(resultSet.getBoolean(1));
        Assert.assertFalse(resultSet.getBoolean("STRING"));
    }

    @Test
    public void testGetURL() throws SQLException, IOException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("STRING"), Collections.singletonList(ColumnType.STRING)),
                Collections.singletonList(new Object[]{"https://google.com"}));
        Assert.assertTrue(resultSet.next());

        Assert.assertEquals(new URL("https://google.com"), resultSet.getURL(1));
        Assert.assertEquals(new URL("https://google.com"), resultSet.getURL("STRING"));
    }

    @Test
    public void testGetBytes() throws SQLException, IOException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("BINARY"), Collections.singletonList(ColumnType.DOUBLE)),
                Collections.singletonList(new Object[]{ByteBuffer.wrap("1688".getBytes())}));
        Assert.assertTrue(resultSet.next());

        // test long
        Assert.assertThrows(SQLException.class, () -> resultSet.getByte(1));
        Assert.assertThrows(SQLException.class, () -> resultSet.getByte("BINARY"));
        Assert.assertEquals(1688, resultSet.getShort(1));
        Assert.assertEquals(1688, resultSet.getShort("BINARY"));
        Assert.assertEquals(1688, resultSet.getInt(1));
        Assert.assertEquals(1688, resultSet.getInt("BINARY"));
        Assert.assertEquals(1688, resultSet.getLong(1));
        Assert.assertEquals(1688, resultSet.getLong("BINARY"));
        Assert.assertEquals(new BigDecimal(1688), resultSet.getBigDecimal(1));
        Assert.assertEquals(new BigDecimal(1688), resultSet.getBigDecimal("BINARY"));

        // test double
        Assert.assertEquals(1688f, resultSet.getFloat(1), (float) EPSILON);
        Assert.assertEquals(1688f, resultSet.getFloat("BINARY"), (float) EPSILON);
        Assert.assertEquals(1688, resultSet.getDouble(1), EPSILON);
        Assert.assertEquals(1688, resultSet.getDouble("BINARY"), EPSILON);

        // test string
        Assert.assertEquals("1688", resultSet.getString(1));
        Assert.assertEquals("1688", resultSet.getString("BINARY"));
        Assert.assertEquals("1688", resultSet.getNString(1));
        Assert.assertEquals("1688", resultSet.getNString("BINARY"));
        Assert.assertEquals("1688", readString(resultSet.getCharacterStream(1)));
        Assert.assertEquals("1688", readString(resultSet.getCharacterStream("BINARY")));
        Assert.assertEquals("1688", readString(resultSet.getNCharacterStream(1)));
        Assert.assertEquals("1688", readString(resultSet.getNCharacterStream("BINARY")));
        Assert.assertEquals("1688", readString(resultSet.getClob(1).getCharacterStream()));
        Assert.assertEquals("1688", readString(resultSet.getClob("BINARY").getCharacterStream()));

        // test bytes
        Assert.assertArrayEquals("1688".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("1688".getBytes(), resultSet.getBytes("BINARY"));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream(1)));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream("BINARY")));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream(1)));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream("BINARY")));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBlob(1).getBinaryStream()));
        Assert.assertArrayEquals("1688".getBytes(), IOUtils.toByteArray(resultSet.getBlob("BINARY").getBinaryStream()));

        // test boolean
        Assert.assertFalse(resultSet.getBoolean(1));
        Assert.assertFalse(resultSet.getBoolean("BINARY"));
    }

    @Test
    public void testGetNull() throws SQLException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("NULL"), Collections.singletonList(ColumnType.STRING)),
                Collections.singletonList(new Object[]{null}));
        Assert.assertTrue(resultSet.next());

        // test long
        Assert.assertEquals(0, resultSet.getByte(1));
        Assert.assertEquals(0, resultSet.getByte("NULL"));
        Assert.assertEquals(0, resultSet.getShort(1));
        Assert.assertEquals(0, resultSet.getShort("NULL"));
        Assert.assertEquals(0, resultSet.getInt(1));
        Assert.assertEquals(0, resultSet.getInt("NULL"));
        Assert.assertEquals(0, resultSet.getLong(1));
        Assert.assertEquals(0, resultSet.getLong("NULL"));
        Assert.assertNull(resultSet.getBigDecimal(1));
        Assert.assertNull(resultSet.getBigDecimal("NULL"));
        Assert.assertTrue(resultSet.wasNull());

        // test double
        Assert.assertEquals(0f, resultSet.getFloat(1), (float) EPSILON);
        Assert.assertEquals(0f, resultSet.getFloat("NULL"), (float) EPSILON);
        Assert.assertEquals(0, resultSet.getDouble(1), EPSILON);
        Assert.assertEquals(0, resultSet.getDouble("NULL"), EPSILON);
        Assert.assertTrue(resultSet.wasNull());

        // test string
        Assert.assertNull(resultSet.getString(1));
        Assert.assertNull(resultSet.getString("NULL"));
        Assert.assertNull(resultSet.getNString(1));
        Assert.assertNull(resultSet.getNString("NULL"));
        Assert.assertNull(resultSet.getCharacterStream(1));
        Assert.assertNull(resultSet.getCharacterStream("NULL"));
        Assert.assertNull(resultSet.getNCharacterStream(1));
        Assert.assertNull(resultSet.getNCharacterStream("NULL"));
        Assert.assertNull(resultSet.getURL(1));
        Assert.assertNull(resultSet.getURL("NULL"));
        Assert.assertTrue(resultSet.wasNull());

        // test bytes
        Assert.assertNull(resultSet.getBytes(1));
        Assert.assertNull(resultSet.getBytes("NULL"));
        Assert.assertTrue(resultSet.wasNull());

        // test boolean
        Assert.assertFalse(resultSet.getBoolean(1));
        Assert.assertFalse(resultSet.getBoolean("NULL"));
        Assert.assertTrue(resultSet.wasNull());
    }

    @Test
    public void testDateTime() throws SQLException, IOException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("DATETIME"), Collections.singletonList(ColumnType.DATETIME)),
                Collections.singletonList(new Object[]{ZonedDateTime.ofInstant(Instant.ofEpochMilli(1546300800000L), ZoneId.of("UTC+8"))}));
        Assert.assertTrue(resultSet.next());

        // test string
        Assert.assertEquals("2019-01-01 08:00:00.0", resultSet.getString(1));
        Assert.assertEquals("2019-01-01 08:00:00.0", resultSet.getString("DATETIME"));
        Assert.assertEquals("2019-01-01 08:00:00.0", resultSet.getNString(1));
        Assert.assertEquals("2019-01-01 08:00:00.0", resultSet.getNString("DATETIME"));
        Assert.assertEquals("2019-01-01 08:00:00.0", readString(resultSet.getCharacterStream(1)));
        Assert.assertEquals("2019-01-01 08:00:00.0", readString(resultSet.getCharacterStream("DATETIME")));
        Assert.assertEquals("2019-01-01 08:00:00.0", readString(resultSet.getNCharacterStream(1)));
        Assert.assertEquals("2019-01-01 08:00:00.0", readString(resultSet.getNCharacterStream("DATETIME")));
        Assert.assertEquals("2019-01-01 08:00:00.0", readString(resultSet.getClob(1).getCharacterStream()));
        Assert.assertEquals("2019-01-01 08:00:00.0", readString(resultSet.getClob("DATETIME").getCharacterStream()));

        // test bytes
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), resultSet.getBytes("DATETIME"));
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream(1)));
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream("DATETIME")));
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream(1)));
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream("DATETIME")));
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), IOUtils.toByteArray(resultSet.getBlob(1).getBinaryStream()));
        Assert.assertArrayEquals("2019-01-01 08:00:00.0".getBytes(), IOUtils.toByteArray(resultSet.getBlob("DATETIME").getBinaryStream()));

        // test datetime
        Assert.assertEquals(new Timestamp(1546300800000L), resultSet.getTimestamp(1));
        Assert.assertEquals(new Timestamp(1546300800000L), resultSet.getTimestamp("DATETIME"));
    }

    @Test
    public void testTime() throws SQLException, IOException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("TIME"), Collections.singletonList(ColumnType.TIME)),
                Collections.singletonList(new Object[]{Duration.parse("PT12H34M56S")}));
        Assert.assertTrue(resultSet.next());

        // test string
        Assert.assertEquals("12:34:56", resultSet.getString(1));
        Assert.assertEquals("12:34:56", resultSet.getString("TIME"));
        Assert.assertEquals("12:34:56", resultSet.getNString(1));
        Assert.assertEquals("12:34:56", resultSet.getNString("TIME"));
        Assert.assertEquals("12:34:56", readString(resultSet.getCharacterStream(1)));
        Assert.assertEquals("12:34:56", readString(resultSet.getCharacterStream("TIME")));
        Assert.assertEquals("12:34:56", readString(resultSet.getNCharacterStream(1)));
        Assert.assertEquals("12:34:56", readString(resultSet.getNCharacterStream("TIME")));
        Assert.assertEquals("12:34:56", readString(resultSet.getClob(1).getCharacterStream()));
        Assert.assertEquals("12:34:56", readString(resultSet.getClob("TIME").getCharacterStream()));

        // test bytes
        Assert.assertArrayEquals("12:34:56".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("12:34:56".getBytes(), resultSet.getBytes("TIME"));
        Assert.assertArrayEquals("12:34:56".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream(1)));
        Assert.assertArrayEquals("12:34:56".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream("TIME")));
        Assert.assertArrayEquals("12:34:56".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream(1)));
        Assert.assertArrayEquals("12:34:56".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream("TIME")));
        Assert.assertArrayEquals("12:34:56".getBytes(), IOUtils.toByteArray(resultSet.getBlob(1).getBinaryStream()));
        Assert.assertArrayEquals("12:34:56".getBytes(), IOUtils.toByteArray(resultSet.getBlob("TIME").getBinaryStream()));

        // test time
        Assert.assertEquals(Time.valueOf("12:34:56"), resultSet.getTime(1));
        Assert.assertEquals(Time.valueOf("12:34:56"), resultSet.getTime("TIME"));
    }

    @Test
    public void testDate() throws SQLException, IOException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("DATE"), Collections.singletonList(ColumnType.DATE)),
                Collections.singletonList(new Object[]{LocalDate.parse("2019-01-01")}));
        Assert.assertTrue(resultSet.next());

        // test string
        Assert.assertEquals("2019-01-01", resultSet.getString(1));
        Assert.assertEquals("2019-01-01", resultSet.getString("DATE"));
        Assert.assertEquals("2019-01-01", resultSet.getNString(1));
        Assert.assertEquals("2019-01-01", resultSet.getNString("DATE"));
        Assert.assertEquals("2019-01-01", readString(resultSet.getCharacterStream(1)));
        Assert.assertEquals("2019-01-01", readString(resultSet.getCharacterStream("DATE")));
        Assert.assertEquals("2019-01-01", readString(resultSet.getNCharacterStream(1)));
        Assert.assertEquals("2019-01-01", readString(resultSet.getNCharacterStream("DATE")));
        Assert.assertEquals("2019-01-01", readString(resultSet.getClob(1).getCharacterStream()));
        Assert.assertEquals("2019-01-01", readString(resultSet.getClob("DATE").getCharacterStream()));

        // test bytes
        Assert.assertArrayEquals("2019-01-01".getBytes(), resultSet.getBytes(1));
        Assert.assertArrayEquals("2019-01-01".getBytes(), resultSet.getBytes("DATE"));
        Assert.assertArrayEquals("2019-01-01".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream(1)));
        Assert.assertArrayEquals("2019-01-01".getBytes(), IOUtils.toByteArray(resultSet.getAsciiStream("DATE")));
        Assert.assertArrayEquals("2019-01-01".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream(1)));
        Assert.assertArrayEquals("2019-01-01".getBytes(), IOUtils.toByteArray(resultSet.getBinaryStream("DATE")));
        Assert.assertArrayEquals("2019-01-01".getBytes(), IOUtils.toByteArray(resultSet.getBlob(1).getBinaryStream()));
        Assert.assertArrayEquals("2019-01-01".getBytes(), IOUtils.toByteArray(resultSet.getBlob("DATE").getBinaryStream()));

        // test date
        Assert.assertEquals(Date.valueOf("2019-01-01"), resultSet.getDate(1));
        Assert.assertEquals(Date.valueOf("2019-01-01"), resultSet.getDate("DATE"));
    }

    @Test
    public void testParseDateTime() throws SQLException {
        ResultSet resultSet = new OTSResultSet(
                new OTSResultSetMetaData(Collections.singletonList("STRING"), Collections.singletonList(ColumnType.STRING)),
                Arrays.asList(new Object[]{"2019-01-01 12:34:56"}, new Object[]{"12:34:56"}, new Object[]{"2019-01-01"}));

        // test datetime
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(Timestamp.valueOf("2019-01-01 12:34:56"), resultSet.getTimestamp(1));
        Assert.assertEquals(Timestamp.valueOf("2019-01-01 12:34:56"), resultSet.getTimestamp("STRING"));

        // test time
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(Time.valueOf("12:34:56"), resultSet.getTime(1));
        Assert.assertEquals(Time.valueOf("12:34:56"), resultSet.getTime("STRING"));

        // test date
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals(Date.valueOf("2019-01-01"), resultSet.getDate(1));
        Assert.assertEquals(Date.valueOf("2019-01-01"), resultSet.getDate("STRING"));
    }

    @Test
    public void testGetTypesNotSupported() throws SQLException {
        ResultSet resultSet = new OTSResultSet(new OTSResultSetMetaData(new ArrayList<>(), new ArrayList<>()), null);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBigDecimal(1, 1));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBigDecimal(null, 1));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId(1));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId(null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML(1));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML(null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRef(1));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRef(null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNClob(1));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNClob(null));
    }

    @Test
    public void testUpdate() throws SQLException {
        ResultSet resultSet = new OTSResultSet(new OTSResultSetMetaData(new ArrayList<>(), new ArrayList<>()), null);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null, 0L));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(null, null, 0L));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null, 0L));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(null, null, 0L));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, (Blob) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(null, (Blob) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, (InputStream) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(null, (InputStream) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(1, false));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(null, false));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte(1, (byte) 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte(null, (byte) 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null, 0L));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(null, null, 0L));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, (Clob) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(null, (Clob) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, (Reader) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(null, (Reader) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(1, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(1, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt(1, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt(null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong(1, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong(null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, (NClob) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(null, (NClob) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, (Reader) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(null, (Reader) null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull(null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull(1));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(null, null, 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, resultSet::updateRow);
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort(1, (short) 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort(null, (short) 0));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime(null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp(null, null));
    }
}
