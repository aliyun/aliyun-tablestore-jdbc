package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OTSConnectionConfigurationTest {

    private static final String[][] properties = new String[][]{
            new String[]{OTSConnection.IO_THREAD_COUNT, "1"},
            new String[]{OTSConnection.MAX_CONNECTIONS, "2"},
            new String[]{OTSConnection.SOCKET_TIMEOUT_IN_MILLISECOND, "4"},
            new String[]{OTSConnection.CONNECTION_TIMEOUT_IN_MILLISECOND, "8"},
            new String[]{OTSConnection.CONNECTION_REQUEST_TIMEOUT_IN_MILLISECOND, "16"},
            new String[]{OTSConnection.RETRY_THREAD_COUNT, "32"},
            new String[]{OTSConnection.ENABLE_REQUEST_COMPRESSION, "true"},
            new String[]{OTSConnection.ENABLE_RESPONSE_COMPRESSION, "true"},
            new String[]{OTSConnection.ENABLE_RESPONSE_VALIDATION, "false"},
            new String[]{OTSConnection.ENABLE_RESPONSE_CONTENT_MD5_CHECKING, "true"},
            new String[]{OTSConnection.TIME_THRESHOLD_OF_SERVER_TRACER, "64"},
            new String[]{OTSConnection.TIME_THRESHOLD_OF_TRACE_LOGGER, "128"},
            new String[]{OTSConnection.PROXY_HOST, "127.0.0.1"},
            new String[]{OTSConnection.PROXY_PORT, "1080"},
            new String[]{OTSConnection.PROXY_USERNAME, "proxy_username"},
            new String[]{OTSConnection.PROXY_PASSWORD, "proxy_password"},
            new String[]{OTSConnection.PROXY_DOMAIN, "example.com"},
            new String[]{OTSConnection.PROXY_WORKSTATION, "proxy_workstation"},
            new String[]{OTSConnection.SYNC_CLIENT_WAIT_FUTURE_TIMEOUT_IN_MILLIS, "256"}
    };

    @BeforeClass
    public static void loadDriver() throws ClassNotFoundException {
        Class.forName(OTSDriver.class.getName());
    }

    @Test
    public void testGetConnection() throws SQLException {
        // Missing access key id
        try {
            DriverManager.getConnection("jdbc:ots:https://example.com/");
            Assert.fail("Exception is expected.");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Access key id should not be null or empty.");
        }

        // Missing access key secret
        try {
            DriverManager.getConnection("jdbc:ots:https://access_key_id@example.com/");
            Assert.fail("Exception is expected.");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Access key secret should not be null or empty.");
        }

        // Missing instance name
        try {
            DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/");
            Assert.fail("Exception is expected.");
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "The name of instance should not be null or empty.");
        }

        // Connection with default configuration
        Connection connection = DriverManager.getConnection("jdbc:ots:https://example.com/instance_name", "access_key_id", "access_key_secret");
        Assert.assertTrue(connection instanceof OTSConnection);
        OTSConnectionConfiguration configuration = ((OTSConnection) connection).config;
        Assert.assertEquals("access_key_id", configuration.getAccessKeyId());
        Assert.assertEquals("access_key_secret", configuration.getAccessKeySecret());
        Assert.assertEquals("https://example.com", configuration.getEndPoint());
        Assert.assertEquals("instance_name", configuration.getInstanceName());
        ClientConfiguration defaultConfiguration = new ClientConfiguration();
        ClientConfiguration clientConfiguration = configuration.getClientConfiguration();
        Assert.assertEquals(defaultConfiguration.getIoThreadCount(), clientConfiguration.getIoThreadCount());
        Assert.assertEquals(defaultConfiguration.getMaxConnections(), clientConfiguration.getMaxConnections());
        Assert.assertEquals(defaultConfiguration.getSocketTimeoutInMillisecond(), clientConfiguration.getSocketTimeoutInMillisecond());
        Assert.assertEquals(defaultConfiguration.getConnectionTimeoutInMillisecond(), clientConfiguration.getConnectionTimeoutInMillisecond());
        Assert.assertEquals(defaultConfiguration.getConnectionRequestTimeoutInMillisecond(), clientConfiguration.getConnectionRequestTimeoutInMillisecond());
        Assert.assertEquals(defaultConfiguration.getRetryThreadCount(), clientConfiguration.getRetryThreadCount());
        Assert.assertEquals(defaultConfiguration.isEnableRequestCompression(), clientConfiguration.isEnableRequestCompression());
        Assert.assertEquals(defaultConfiguration.isEnableResponseCompression(), clientConfiguration.isEnableResponseCompression());
        Assert.assertEquals(defaultConfiguration.isEnableResponseValidation(), clientConfiguration.isEnableResponseValidation());
        Assert.assertEquals(defaultConfiguration.isEnableResponseContentMD5Checking(), clientConfiguration.isEnableResponseContentMD5Checking());
        Assert.assertEquals(defaultConfiguration.getTimeThresholdOfServerTracer(), clientConfiguration.getTimeThresholdOfServerTracer());
        Assert.assertEquals(defaultConfiguration.getTimeThresholdOfTraceLogger(), clientConfiguration.getTimeThresholdOfTraceLogger());
        Assert.assertEquals(defaultConfiguration.getProxyHost(), clientConfiguration.getProxyHost());
        Assert.assertEquals(defaultConfiguration.getProxyPort(), clientConfiguration.getProxyPort());
        Assert.assertEquals(defaultConfiguration.getProxyUsername(), clientConfiguration.getProxyUsername());
        Assert.assertEquals(defaultConfiguration.getProxyPassword(), clientConfiguration.getProxyPassword());
        Assert.assertEquals(defaultConfiguration.getProxyDomain(), clientConfiguration.getProxyDomain());
        Assert.assertEquals(defaultConfiguration.getProxyWorkstation(), clientConfiguration.getProxyWorkstation());
        Assert.assertEquals(defaultConfiguration.getSyncClientWaitFutureTimeoutInMillis(), clientConfiguration.getSyncClientWaitFutureTimeoutInMillis());

        // Connection with customized port
        connection = DriverManager.getConnection("jdbc:ots:https://example.com:6666/instance_name", "access_key_id", "access_key_secret");
        Assert.assertTrue(connection instanceof OTSConnection);
        configuration = ((OTSConnection) connection).config;
        Assert.assertEquals("https://example.com:6666", configuration.getEndPoint());
    }

    @Test
    public void testGetConnectionWithQuery() throws SQLException {
        StringBuilder builder = new StringBuilder("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name?");
        for (String[] property : properties) {
            builder.append(property[0]);
            builder.append('=');
            builder.append(property[1]);
            builder.append('&');
        }

        Connection connection = DriverManager.getConnection(builder.toString());
        Assert.assertTrue(connection instanceof OTSConnection);
        OTSConnectionConfiguration configuration = ((OTSConnection) connection).config;
        Assert.assertEquals("access_key_id", configuration.getAccessKeyId());
        Assert.assertEquals("access_key_secret", configuration.getAccessKeySecret());
        Assert.assertEquals("https://example.com", configuration.getEndPoint());
        Assert.assertEquals("instance_name", configuration.getInstanceName());
        ClientConfiguration clientConfiguration = configuration.getClientConfiguration();
        Assert.assertEquals(1, clientConfiguration.getIoThreadCount());
        Assert.assertEquals(2, clientConfiguration.getMaxConnections());
        Assert.assertEquals(4, clientConfiguration.getSocketTimeoutInMillisecond());
        Assert.assertEquals(8, clientConfiguration.getConnectionTimeoutInMillisecond());
        Assert.assertEquals(16, clientConfiguration.getConnectionRequestTimeoutInMillisecond());
        Assert.assertEquals(32, clientConfiguration.getRetryThreadCount());
        Assert.assertTrue(clientConfiguration.isEnableRequestCompression());
        Assert.assertTrue(clientConfiguration.isEnableResponseCompression());
        Assert.assertFalse(clientConfiguration.isEnableResponseValidation());
        Assert.assertTrue(clientConfiguration.isEnableResponseContentMD5Checking());
        Assert.assertEquals(64, clientConfiguration.getTimeThresholdOfServerTracer());
        Assert.assertEquals(128, clientConfiguration.getTimeThresholdOfTraceLogger());
        Assert.assertEquals("127.0.0.1", clientConfiguration.getProxyHost());
        Assert.assertEquals(1080, clientConfiguration.getProxyPort());
        Assert.assertEquals("proxy_username", clientConfiguration.getProxyUsername());
        Assert.assertEquals("proxy_password", clientConfiguration.getProxyPassword());
        Assert.assertEquals("example.com", clientConfiguration.getProxyDomain());
        Assert.assertEquals("proxy_workstation", clientConfiguration.getProxyWorkstation());
        Assert.assertEquals(256, clientConfiguration.getSyncClientWaitFutureTimeoutInMillis());
    }

    @Test
    public void testGetConnectionWithProperties() throws SQLException {
        Properties info = new Properties();
        for (String[] property : properties) {
            info.setProperty(property[0], property[1]);
        }

        Connection connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name", info);
        Assert.assertTrue(connection instanceof OTSConnection);
        OTSConnectionConfiguration configuration = ((OTSConnection) connection).config;
        Assert.assertEquals("access_key_id", configuration.getAccessKeyId());
        Assert.assertEquals("access_key_secret", configuration.getAccessKeySecret());
        Assert.assertEquals("https://example.com", configuration.getEndPoint());
        Assert.assertEquals("instance_name", configuration.getInstanceName());
        ClientConfiguration clientConfiguration = configuration.getClientConfiguration();
        Assert.assertEquals(1, clientConfiguration.getIoThreadCount());
        Assert.assertEquals(2, clientConfiguration.getMaxConnections());
        Assert.assertEquals(4, clientConfiguration.getSocketTimeoutInMillisecond());
        Assert.assertEquals(8, clientConfiguration.getConnectionTimeoutInMillisecond());
        Assert.assertEquals(16, clientConfiguration.getConnectionRequestTimeoutInMillisecond());
        Assert.assertEquals(32, clientConfiguration.getRetryThreadCount());
        Assert.assertTrue(clientConfiguration.isEnableRequestCompression());
        Assert.assertTrue(clientConfiguration.isEnableResponseCompression());
        Assert.assertFalse(clientConfiguration.isEnableResponseValidation());
        Assert.assertTrue(clientConfiguration.isEnableResponseContentMD5Checking());
        Assert.assertEquals(64, clientConfiguration.getTimeThresholdOfServerTracer());
        Assert.assertEquals(128, clientConfiguration.getTimeThresholdOfTraceLogger());
        Assert.assertEquals("127.0.0.1", clientConfiguration.getProxyHost());
        Assert.assertEquals(1080, clientConfiguration.getProxyPort());
        Assert.assertEquals("proxy_username", clientConfiguration.getProxyUsername());
        Assert.assertEquals("proxy_password", clientConfiguration.getProxyPassword());
        Assert.assertEquals("example.com", clientConfiguration.getProxyDomain());
        Assert.assertEquals("proxy_workstation", clientConfiguration.getProxyWorkstation());
        Assert.assertEquals(256, clientConfiguration.getSyncClientWaitFutureTimeoutInMillis());
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        Map<String, String> expectedProperties = new HashMap<>();
        Properties info = new Properties();
        for (String[] property : properties) {
            info.setProperty(property[0], property[1]);
            expectedProperties.put(property[0], property[1]);
        }
        Driver driver = DriverManager.getDriver("jdbc:ots:");
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name", info);
        Assert.assertEquals(expectedProperties.size(), driverPropertyInfos.length);
        for (DriverPropertyInfo driverPropertyInfo : driverPropertyInfos) {
            Assert.assertEquals(String.format("%s doesn't match.", driverPropertyInfo.name),
                    expectedProperties.get(driverPropertyInfo.name), driverPropertyInfo.value);
        }
    }

    @Test
    public void testSetNetworkTimeout() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name");
        Assert.assertTrue(connection instanceof OTSConnection);
        Assert.assertEquals(-1, connection.getNetworkTimeout());
        connection.setNetworkTimeout(null, 1000);
        Assert.assertEquals(-1, connection.getNetworkTimeout());
    }

    @Test
    public void testSetRetryStrategy() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name");
        Assert.assertTrue(connection instanceof OTSConnection);
        OTSConnectionConfiguration configuration = ((OTSConnection) connection).config;
        Assert.assertTrue(configuration.getClientConfiguration().getRetryStrategy() instanceof DefaultRetryStrategy);
        // disable retry strategy
        Properties info = new Properties();
        info.setProperty(OTSConnection.RETRY_STRATEGY, "disable");
        connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name", info);
        Assert.assertTrue(connection instanceof OTSConnection);
        configuration = ((OTSConnection) connection).config;
        Assert.assertTrue(configuration.getClientConfiguration().getRetryStrategy() instanceof DisableRetryStrategy);
        // default retry strategy
        info = new Properties();
        info.setProperty(OTSConnection.RETRY_STRATEGY, "default");
        connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name", info);
        Assert.assertTrue(connection instanceof OTSConnection);
        configuration = ((OTSConnection) connection).config;
        Assert.assertTrue(configuration.getClientConfiguration().getRetryStrategy() instanceof DefaultRetryStrategy);
    }
}
