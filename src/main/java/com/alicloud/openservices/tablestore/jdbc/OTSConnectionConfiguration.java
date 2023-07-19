package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.ClientConfiguration;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Properties;

class OTSConnectionConfiguration {

    private String endPoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String instanceName;
    private ClientConfiguration clientConfiguration;

    static OTSConnectionConfiguration parse(String url, Properties info) throws SQLException {
        assert url != null;
        assert info != null;
        assert url.startsWith(OTSDriver.OTS_JDBC_URL_PREFIX);

        // Parse URL.
        URL parsed;
        try {
            parsed = new URL(url.substring(OTSDriver.OTS_JDBC_URL_PREFIX.length()));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
        OTSConnectionConfiguration configuration = new OTSConnectionConfiguration();
        configuration.endPoint = parsed.getProtocol() + "://" + parsed.getHost();
        String userInfo = parsed.getUserInfo();
        if (userInfo != null) {
            String[] userPass = userInfo.split(":");
            configuration.accessKeyId = userPass[0];
            if (userPass.length >= 2) {
                configuration.accessKeySecret = userPass[1];
            }
        }
        String path = parsed.getPath();
        if (path != null && !path.isEmpty()) {
            configuration.instanceName = path.substring(1);
        }
        String query = parsed.getQuery();
        if (query != null) {
            String[] params = query.split("&");
            for (String param : params) {
                String[] keyValue = param.split("=");
                if (keyValue.length == 2) {
                    info.setProperty(keyValue[0], keyValue[1]);
                }
            }
        }

        // Parse properties.
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        configuration.accessKeyId = info.getProperty(OTSConnection.ACCESS_KEY_ID, configuration.accessKeyId);
        configuration.accessKeySecret = info.getProperty(OTSConnection.ACCESS_KEY_SECRET, configuration.accessKeySecret);
        clientConfiguration.setEnableRequestCompression(Boolean.parseBoolean(info.getProperty(
                OTSConnection.ENABLE_REQUEST_COMPRESSION,
                String.valueOf(clientConfiguration.isEnableRequestCompression()))));
        clientConfiguration.setEnableResponseCompression(Boolean.parseBoolean(info.getProperty(
                OTSConnection.ENABLE_RESPONSE_COMPRESSION,
                String.valueOf(clientConfiguration.isEnableResponseCompression()))));
        clientConfiguration.setEnableResponseValidation(Boolean.parseBoolean(info.getProperty(
                OTSConnection.ENABLE_RESPONSE_VALIDATION,
                String.valueOf(clientConfiguration.isEnableResponseValidation()))));
        clientConfiguration.setIoThreadCount(Integer.parseInt(info.getProperty(
                OTSConnection.IO_THREAD_COUNT,
                String.valueOf(clientConfiguration.getIoThreadCount()))));
        clientConfiguration.setMaxConnections(Integer.parseInt(info.getProperty(
                OTSConnection.MAX_CONNECTIONS,
                String.valueOf(clientConfiguration.getMaxConnections()))));
        clientConfiguration.setSocketTimeoutInMillisecond(Integer.parseInt(info.getProperty(
                OTSConnection.SOCKET_TIMEOUT_IN_MILLISECOND,
                String.valueOf(clientConfiguration.getSocketTimeoutInMillisecond()))));
        clientConfiguration.setConnectionTimeoutInMillisecond(Integer.parseInt(info.getProperty(
                OTSConnection.CONNECTION_TIMEOUT_IN_MILLISECOND,
                String.valueOf(clientConfiguration.getConnectionTimeoutInMillisecond()))));
        clientConfiguration.setRetryThreadCount(Integer.parseInt(info.getProperty(
                OTSConnection.RETRY_THREAD_COUNT,
                String.valueOf(clientConfiguration.getRetryThreadCount()))));
        clientConfiguration.setEnableResponseContentMD5Checking(Boolean.parseBoolean(info.getProperty(
                OTSConnection.ENABLE_RESPONSE_CONTENT_MD5_CHECKING,
                String.valueOf(clientConfiguration.isEnableResponseContentMD5Checking()))));
        clientConfiguration.setTimeThresholdOfTraceLogger(Integer.parseInt(info.getProperty(
                OTSConnection.TIME_THRESHOLD_OF_TRACE_LOGGER,
                String.valueOf(clientConfiguration.getTimeThresholdOfTraceLogger()))));
        clientConfiguration.setTimeThresholdOfServerTracer(Integer.parseInt(info.getProperty(
                OTSConnection.TIME_THRESHOLD_OF_SERVER_TRACER,
                String.valueOf(clientConfiguration.getTimeThresholdOfServerTracer()))));
        clientConfiguration.setProxyHost(info.getProperty(
                OTSConnection.PROXY_HOST));
        clientConfiguration.setProxyPort(Integer.parseInt(info.getProperty(
                OTSConnection.PROXY_PORT,
                String.valueOf(clientConfiguration.getProxyPort()))));
        clientConfiguration.setProxyUsername(info.getProperty(
                OTSConnection.PROXY_USERNAME));
        clientConfiguration.setProxyPassword(info.getProperty(
                OTSConnection.PROXY_PASSWORD));
        clientConfiguration.setProxyDomain(info.getProperty(
                OTSConnection.PROXY_DOMAIN));
        clientConfiguration.setProxyWorkstation(info.getProperty(
                OTSConnection.PROXY_WORKSTATION));
        clientConfiguration.setSyncClientWaitFutureTimeoutInMillis(Long.parseLong(info.getProperty(
                OTSConnection.SYNC_CLIENT_WAIT_FUTURE_TIMEOUT_IN_MILLIS,
                String.valueOf(clientConfiguration.getSyncClientWaitFutureTimeoutInMillis()))));
        clientConfiguration.setConnectionRequestTimeoutInMillisecond(Integer.parseInt(info.getProperty(
                OTSConnection.CONNECTION_REQUEST_TIMEOUT_IN_MILLISECOND,
                String.valueOf(clientConfiguration.getConnectionRequestTimeoutInMillisecond()))));
        configuration.clientConfiguration = clientConfiguration;
        return configuration;
    }

    ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    String getAccessKeyId() {
        return accessKeyId;
    }

    String getAccessKeySecret() {
        return accessKeySecret;
    }

    String getEndPoint() {
        return endPoint;
    }

    String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
}
