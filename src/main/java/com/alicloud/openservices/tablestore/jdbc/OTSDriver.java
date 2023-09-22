package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.ClientConfiguration;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class OTSDriver implements Driver {

    static final String OTS_JDBC_URL_PREFIX = "jdbc:ots:";
    static final boolean OTS_JDBC_COMPLIANT = false;
    static final int OTS_JDBC_MAJOR_VERSION = 4;
    static final int OTS_JDBC_MINOR_VERSION = 2;
    static final int OTS_DRIVER_MAJOR_VERSION = 5;
    static final int OTS_DRIVER_MINOR_VERSION = 16;
    static final String OTS_DRIVER_VERSION = "5.16.1";

    static {
        try {
            DriverManager.registerDriver(new OTSDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return acceptsURL(url) ? new OTSConnection(url, info) : null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(OTS_JDBC_URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        ClientConfiguration configuration = OTSConnectionConfiguration.parse(url, info).getClientConfiguration();
        return new DriverPropertyInfo[]{
                new DriverPropertyInfo(OTSConnection.ENABLE_REQUEST_COMPRESSION, String.valueOf(configuration.isEnableRequestCompression())),
                new DriverPropertyInfo(OTSConnection.ENABLE_RESPONSE_COMPRESSION, String.valueOf(configuration.isEnableResponseCompression())),
                new DriverPropertyInfo(OTSConnection.ENABLE_RESPONSE_VALIDATION, String.valueOf(configuration.isEnableResponseValidation())),
                new DriverPropertyInfo(OTSConnection.IO_THREAD_COUNT, String.valueOf(configuration.getIoThreadCount())),
                new DriverPropertyInfo(OTSConnection.MAX_CONNECTIONS, String.valueOf(configuration.getMaxConnections())),
                new DriverPropertyInfo(OTSConnection.SOCKET_TIMEOUT_IN_MILLISECOND, String.valueOf(configuration.getSocketTimeoutInMillisecond())),
                new DriverPropertyInfo(OTSConnection.CONNECTION_TIMEOUT_IN_MILLISECOND, String.valueOf(configuration.getConnectionTimeoutInMillisecond())),
                new DriverPropertyInfo(OTSConnection.RETRY_THREAD_COUNT, String.valueOf(configuration.getRetryThreadCount())),
                new DriverPropertyInfo(OTSConnection.ENABLE_RESPONSE_CONTENT_MD5_CHECKING, String.valueOf(configuration.isEnableResponseContentMD5Checking())),
                new DriverPropertyInfo(OTSConnection.TIME_THRESHOLD_OF_SERVER_TRACER, String.valueOf(configuration.getTimeThresholdOfServerTracer())),
                new DriverPropertyInfo(OTSConnection.TIME_THRESHOLD_OF_TRACE_LOGGER, String.valueOf(configuration.getTimeThresholdOfTraceLogger())),
                new DriverPropertyInfo(OTSConnection.PROXY_HOST, configuration.getProxyHost()),
                new DriverPropertyInfo(OTSConnection.PROXY_PORT, String.valueOf(configuration.getProxyPort())),
                new DriverPropertyInfo(OTSConnection.PROXY_USERNAME, configuration.getProxyUsername()),
                new DriverPropertyInfo(OTSConnection.PROXY_PASSWORD, configuration.getProxyPassword()),
                new DriverPropertyInfo(OTSConnection.PROXY_DOMAIN, configuration.getProxyDomain()),
                new DriverPropertyInfo(OTSConnection.PROXY_WORKSTATION, configuration.getProxyWorkstation()),
                new DriverPropertyInfo(OTSConnection.SYNC_CLIENT_WAIT_FUTURE_TIMEOUT_IN_MILLIS, String.valueOf(configuration.getSyncClientWaitFutureTimeoutInMillis())),
                new DriverPropertyInfo(OTSConnection.CONNECTION_REQUEST_TIMEOUT_IN_MILLISECOND, String.valueOf(configuration.getConnectionRequestTimeoutInMillisecond()))
        };
    }

    @Override
    public int getMajorVersion() {
        return OTS_DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return OTS_DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return OTS_JDBC_COMPLIANT;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
