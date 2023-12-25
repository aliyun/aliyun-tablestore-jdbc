# Aliyun TableStore JDBC Driver

阿里云表格存储服务的JDBC驱动。

![Maven Central](https://img.shields.io/maven-central/v/com.aliyun.openservices/tablestore-jdbc)

## 安装

- 如果直接使用jar包，请从[Maven仓库](https://repo1.maven.org/maven2/com/aliyun/openservices/tablestore-jdbc/)下载。
- 如果在Maven项目使用，那么添加下面的依赖项：

```xml
<dependency>
  <groupId>com.aliyun.openservices</groupId>
  <artifactId>tablestore-jdbc</artifactId>
  <version>5.16.3</version>
</dependency>
```

## 使用

1. 使用`Class.forName()`加载表格存储JDBC驱动：

```java
Class.forName("com.alicloud.openservices.tablestore.jdbc.OTSDriver");
```

2. 连接数据库，创建连接：

```java
Connection conn = DriverManager.getConnection("jdbc:ots:https://endpoint/instance_name", "access_key_id", "access_key_secret");
```

表格存储JDBC的URL定义如下，方括号之内为可选项：

```
jdbc:ots:schema://[access_key_id:access_key_secret@]endpoint/instance_name[?param1=value1&...&paramN=valueN]
```

- `schema`是表格存储JDBC驱动使用的协议，通常是`https`
- `access_key_id`是表格存储的AccessKey ID
- `access_key_secret`是表格存储的AccessKey Secret
- `endpoint`为表格存储服务的域名地址
- `instance_name`为表格存储服务的实例名称
- URL最后是一些配置项

其中`access_key_id`、`access_key_secret` 和配置项既可以写在URL中，也可以选择写在配置项中。

## 配置项

| 名称 | 默认值 | 说明 |
|---|---|---|
| `user` | 空 | AccessKey ID |
| `password` | 空 | AccessKey Secret |
| `enableRequestCompression` | `false` | 压缩请求数据 |
| `enableResponseCompression` | `false` | 压缩响应数据 |
| `enableResponseValidation` | `true` | 验证响应数据 |
| `ioThreadCount` | CPU核心数量 | HttpAsyncClient的IOReactor的线程数 |
| `maxConnections` | `300` | 允许打开的最大HTTP连接数 |
| `socketTimeoutInMillisecond` | `30000` | Socket层传输数据的超时时间，0表示无限等待（单位：毫秒） |
| `connectionTimeoutInMillisecond` | `30000` | 建立连接的超时时间，0表示无限等待（单位：毫秒） |
| `retryThreadCount` | `1` | 用于执行错误重试的线程池的线程的个数 |
| `enableResponseContentMD5Checking` | `false` | 验证响应数据的MD5 |
| `timeThresholdOfTraceLogger` | `1000` | |
| `timeThresholdOfServerTracer` | `500` | |
| `proxyHost` | 空 | 代理服务器主机地址 |
| `proxyPort` | 空 | 代理服务器端口 |
| `proxyUsername` | 空 | 代理服务器验证的用户名 |
| `proxyPassword` | 空 | 代理服务器验证的密码 |
| `proxyDomain` | 空 | 设置访问NTLM验证的代理服务器的Windows域名 |
| `proxyWorkstation` | 空 | NTLM代理服务器的Windows工作站名称 |
| `syncClientWaitFutureTimeoutInMillis` | `-1` | 异步等待的超时时间（单位：毫秒） |
| `connectionRequestTimeoutInMillisecond` | `60000` | 发送请求的超时时间（单位：毫秒） |

## 贡献代码
- 我们非常欢迎大家为TableStore JDBC驱动以及其他阿里云SDK贡献代码

## 联系我们
- [阿里云TableStore官方网站](http://www.aliyun.com/product/ots)
- [阿里云TableStore官方论坛](http://bbs.aliyun.com)
- [阿里云TableStore官方文档中心](https://help.aliyun.com/product/8315004_ots.html)
- [阿里云云栖社区](http://yq.aliyun.com)
- [阿里云工单系统](https://workorder.console.aliyun.com/#/ticket/createIndex)
