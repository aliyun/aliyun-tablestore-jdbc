package com.alicloud.openservices.tablestore.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class OTSStatementTest {

    @BeforeClass
    public static void loadDriver() throws ClassNotFoundException {
        Class.forName(OTSDriver.class.getName());
    }

    @Test
    public void testSetQueryTimeout() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name");
        Statement statement = connection.createStatement();
        Assert.assertEquals(60, statement.getQueryTimeout());
        // set query timeout to 5 seconds
        statement.setQueryTimeout(5);
        Assert.assertEquals(5, statement.getQueryTimeout());
        // set query timeout to default value
        statement.setQueryTimeout(0);
        Assert.assertEquals(60, statement.getQueryTimeout());
        // set query timeout to negative value
        Assert.assertThrows(SQLException.class, () -> statement.setQueryTimeout(-1));
    }
}
