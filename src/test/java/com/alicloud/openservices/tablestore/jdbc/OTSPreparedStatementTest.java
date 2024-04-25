package com.alicloud.openservices.tablestore.jdbc;

import java.time.Instant;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class OTSPreparedStatementTest {

    @BeforeClass
    public static void loadDriver() throws ClassNotFoundException {
        Class.forName(OTSDriver.class.getName());
    }

    @Test
    public void testFindPlaceholders() throws SQLException {
        List<Integer> placeholders = OTSPreparedStatement.findPlaceholders("SELECT * FROM t WHERE a = ? AND b = ?");
        Assert.assertArrayEquals(new Object[]{26, 36}, placeholders.toArray());
        placeholders = OTSPreparedStatement.findPlaceholders("SELECT * FROM t WHERE a = '?' AND b = ?");
        Assert.assertArrayEquals(new Object[]{38}, placeholders.toArray());
        placeholders = OTSPreparedStatement.findPlaceholders("SELECT * FROM t WHERE a = \"?\" AND b = ?");
        Assert.assertArrayEquals(new Object[]{38}, placeholders.toArray());
        placeholders = OTSPreparedStatement.findPlaceholders("SELECT * FROM t WHERE a = `?` AND b = ?");
        Assert.assertArrayEquals(new Object[]{38}, placeholders.toArray());
    }

    @Test
    public void testInterpolateParameters() throws SQLException, MalformedURLException {
        OTSConnection connection = new OTSConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name", new Properties());
        OTSPreparedStatement statement = new OTSPreparedStatement(connection, "SELECT * FROM t WHERE a = ?");
        Assert.assertEquals("SELECT * FROM t WHERE a = NULL", statement.interpolateParameters());

        // test boolean
        statement.setBoolean(1, true);
        Assert.assertEquals("SELECT * FROM t WHERE a = true", statement.interpolateParameters());

        // test long
        statement.setLong(1, 1);
        Assert.assertEquals("SELECT * FROM t WHERE a = 1", statement.interpolateParameters());
        statement.setByte(1, (byte) 2);
        Assert.assertEquals("SELECT * FROM t WHERE a = 2", statement.interpolateParameters());
        statement.setInt(1, 3);
        Assert.assertEquals("SELECT * FROM t WHERE a = 3", statement.interpolateParameters());
        statement.setShort(1, (short) 4);
        Assert.assertEquals("SELECT * FROM t WHERE a = 4", statement.interpolateParameters());
        statement.setBigDecimal(1, new BigDecimal(5));
        Assert.assertEquals("SELECT * FROM t WHERE a = 5", statement.interpolateParameters());

        // test null
        statement.setNull(1, Types.NULL);
        Assert.assertEquals("SELECT * FROM t WHERE a = NULL", statement.interpolateParameters());
        statement.setNull(1, Types.OTHER, null);
        Assert.assertEquals("SELECT * FROM t WHERE a = NULL", statement.interpolateParameters());

        // test double
        statement.setDouble(1, 1.2);
        Assert.assertEquals("SELECT * FROM t WHERE a = 1.2", statement.interpolateParameters());
        statement.setFloat(1, 2.4f);
        Assert.assertEquals("SELECT * FROM t WHERE a = 2.4000000953674316", statement.interpolateParameters());

        // test string
        statement.setString(1, "true; delete from t; select * from a = true");
        Assert.assertEquals("SELECT * FROM t WHERE a = 'true; delete from t; select * from a = true'", statement.interpolateParameters());
        statement.setNString(1, "\"HELLO\"");
        Assert.assertEquals("SELECT * FROM t WHERE a = '\"HELLO\"'", statement.interpolateParameters());
        statement.setNString(1, " HELLO ");
        Assert.assertEquals("SELECT * FROM t WHERE a = ' HELLO '", statement.interpolateParameters());
        statement.setCharacterStream(1, new CharArrayReader("TableStore".toCharArray()));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setCharacterStream(1, new CharArrayReader("TableStore__".toCharArray()), 10);
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setCharacterStream(1, new CharArrayReader("TableStore__".toCharArray()), 10L);
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setNCharacterStream(1, new CharArrayReader("TableStore".toCharArray()));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setNCharacterStream(1, new CharArrayReader("TableStore__".toCharArray()), 10);
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setClob(1, new SerialClob("TableStore".toCharArray()));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setClob(1, new CharArrayReader("TableStore".toCharArray()));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setClob(1, new CharArrayReader("TableStore__".toCharArray()), 10);
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setNClob(1, new CharArrayReader("TableStore".toCharArray()));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setNClob(1, new CharArrayReader("TableStore__".toCharArray()), 10);
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());

        // test bytes
        statement.setBytes(1, new byte[]{0x40, 0x41, 0x42, 0x43});
        Assert.assertEquals("SELECT * FROM t WHERE a = x'40414243'", statement.interpolateParameters());
        statement.setAsciiStream(1, new ByteArrayInputStream("TableStore".getBytes()));
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setAsciiStream(1, new ByteArrayInputStream("TableStore".getBytes()), 10);
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setAsciiStream(1, new ByteArrayInputStream("TableStore".getBytes()), 10L);
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setBinaryStream(1, new ByteArrayInputStream("TableStore".getBytes()));
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setBinaryStream(1, new ByteArrayInputStream("TableStore__".getBytes()), 10);
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setBinaryStream(1, new ByteArrayInputStream("TableStore__".getBytes()), 10L);
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setUnicodeStream(1, new ByteArrayInputStream("你好".getBytes()), "你好".getBytes().length);
        Assert.assertEquals("SELECT * FROM t WHERE a = x'e4bda0e5a5bd'", statement.interpolateParameters());
        statement.setBlob(1, new SerialBlob("TableStore".getBytes()));
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setBlob(1, new ByteArrayInputStream("TableStore".getBytes()));
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setBlob(1, new ByteArrayInputStream("TableStore__".getBytes()), 10L);
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setURL(1, new URL("https://google.com"));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'https://google.com'", statement.interpolateParameters());

        // test datetime/time/date
        statement.setTimestamp(1, Timestamp.from(Instant.ofEpochSecond(1692841131)));
        Assert.assertEquals("SELECT * FROM t WHERE a = FROM_UNIXTIME(1692841131)", statement.interpolateParameters());
        statement.setTime(1, Time.valueOf("12:34:56"));
        Assert.assertEquals("SELECT * FROM t WHERE a = TIME('12:34:56')", statement.interpolateParameters());
        statement.setDate(1, Date.valueOf("2020-01-01"));
        Assert.assertEquals("SELECT * FROM t WHERE a = DATE('2020-01-01')", statement.interpolateParameters());
    }

    @Test
    public void testSetObject() throws SQLException, MalformedURLException {
        OTSConnection connection = new OTSConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name", new Properties());
        OTSPreparedStatement statement = new OTSPreparedStatement(connection, "SELECT * FROM t WHERE a = ?");
        Assert.assertEquals("SELECT * FROM t WHERE a = NULL", statement.interpolateParameters());

        // test boolean
        statement.setObject(1, true);
        Assert.assertEquals("SELECT * FROM t WHERE a = true", statement.interpolateParameters());

        // test long
        statement.setObject(1, 1L);
        Assert.assertEquals("SELECT * FROM t WHERE a = 1", statement.interpolateParameters());
        statement.setObject(1, (byte) 2);
        Assert.assertEquals("SELECT * FROM t WHERE a = 2", statement.interpolateParameters());
        statement.setObject(1, 3);
        Assert.assertEquals("SELECT * FROM t WHERE a = 3", statement.interpolateParameters());
        statement.setObject(1, (short) 4);
        Assert.assertEquals("SELECT * FROM t WHERE a = 4", statement.interpolateParameters());
        statement.setObject(1, new BigDecimal(5));
        Assert.assertEquals("SELECT * FROM t WHERE a = 5", statement.interpolateParameters());

        // test null
        statement.setObject(1, null);
        Assert.assertEquals("SELECT * FROM t WHERE a = NULL", statement.interpolateParameters());

        // test double
        statement.setObject(1, 1.2);
        Assert.assertEquals("SELECT * FROM t WHERE a = 1.2", statement.interpolateParameters());
        statement.setObject(1, 2.4f);
        Assert.assertEquals("SELECT * FROM t WHERE a = 2.4", statement.interpolateParameters());

        // test string
        statement.setObject(1, "true; delete from t; select * from a = true");
        Assert.assertEquals("SELECT * FROM t WHERE a = 'true; delete from t; select * from a = true'", statement.interpolateParameters());
        statement.setObject(1, new CharArrayReader("TableStore".toCharArray()));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setObject(1, new SerialClob("TableStore".toCharArray()));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'TableStore'", statement.interpolateParameters());
        statement.setObject(1, new URL("https://google.com"));
        Assert.assertEquals("SELECT * FROM t WHERE a = 'https://google.com'", statement.interpolateParameters());

        // test bytes
        statement.setObject(1, new byte[]{0x40, 0x41, 0x42, 0x43});
        Assert.assertEquals("SELECT * FROM t WHERE a = x'40414243'", statement.interpolateParameters());
        statement.setObject(1, new ByteArrayInputStream("TableStore".getBytes()));
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
        statement.setObject(1, new SerialBlob("TableStore".getBytes()));
        Assert.assertEquals("SELECT * FROM t WHERE a = x'5461626c6553746f7265'", statement.interpolateParameters());
    }

    @Test
    public void testSetTypesNotSupported() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name");
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM t WHERE a = ?");
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setArray(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setDate(1, null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setRef(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setRowId(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setSQLXML(1, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setTime(1, null, null));
        Assert.assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setTimestamp(1, null, null));
        Assert.assertThrows(SQLException.class, () -> statement.setObject(1, new ArrayList<>()));
    }
}
