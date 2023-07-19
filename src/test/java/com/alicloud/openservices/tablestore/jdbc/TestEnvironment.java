package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestEnvironment {

    private final String tableName;
    private final String endpoint;
    private final String instanceName;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final SyncClient otsClient;

    TestEnvironment(String name) {
        Map<String, String> env = System.getenv();
        endpoint = env.get("OTS_ENDPOINT");
        instanceName = env.get("OTS_INSTANCE_NAME");
        accessKeyId = env.get("OTS_ACCESS_KEY_ID");
        accessKeySecret = env.get("OTS_ACCESS_KEY_SECRET");
        otsClient = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName);
        tableName = name;
    }

    String getURL() {
        return String.format("jdbc:ots:%s/%s", endpoint, instanceName);
    }

    String getUser() {
        return accessKeyId;
    }

    String getPassword() {
        return accessKeySecret;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getTableName() {
        return tableName;
    }

    void setup() {
        // create table
        dropTableIfExists();
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk3", PrimaryKeyType.BINARY);
        tableMeta.addDefinedColumn("col1", DefinedColumnType.INTEGER);
        tableMeta.addDefinedColumn("col2", DefinedColumnType.DOUBLE);
        tableMeta.addDefinedColumn("col3", DefinedColumnType.STRING);
        tableMeta.addDefinedColumn("col4", DefinedColumnType.BINARY);
        tableMeta.addDefinedColumn("col5", DefinedColumnType.BOOLEAN);
        TableOptions tableOptions = new TableOptions(-1, 1);
        List<IndexMeta> indexMetas = new ArrayList<>();
        IndexMeta indexMeta = new IndexMeta(tableName + "_col1_index");
        indexMeta.addPrimaryKeyColumn("col1");
        indexMeta.addDefinedColumn("col2");
        indexMetas.add(indexMeta);
        indexMeta = new IndexMeta(tableName + "_col3_index");
        indexMeta.addPrimaryKeyColumn("col3");
        indexMeta.addDefinedColumn("col4");
        indexMetas.add(indexMeta);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions, indexMetas);
        otsClient.createTable(createTableRequest);

        // create sql binding
        SQLQueryRequest sqlQueryRequest = new SQLQueryRequest("CREATE TABLE IF NOT EXISTS " + tableName + "(" +
                "pk1 BIGINT," +
                "pk2 VARCHAR(1024)," +
                "pk3 VARBINARY(1024)," +
                "col1 BIGINT," +
                "col2 DOUBLE," +
                "col3 MEDIUMTEXT," +
                "col4 MEDIUMBLOB," +
                "col5 BOOL," +
                "PRIMARY KEY(pk1, pk2, pk3))");
        otsClient.sqlQuery(sqlQueryRequest);

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // insert rows
        for (int i = 1; i <= 3; i++) {
            PrimaryKey primaryKey = new PrimaryKey(new PrimaryKeyColumn[]{
                    new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i)),
                    new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromString(String.valueOf(i))),
                    new PrimaryKeyColumn("pk3", PrimaryKeyValue.fromBinary(new byte[]{(byte) i}))
            });
            RowPutChange change = new RowPutChange(tableName, primaryKey);
            change.addColumn("col1", ColumnValue.fromLong(i));
            change.addColumn("col2", ColumnValue.fromDouble(i));
            change.addColumn("col3", ColumnValue.fromString(String.valueOf(i)));
            change.addColumn("col4", ColumnValue.fromBinary((new byte[]{(byte) i})));
            change.addColumn("col5", ColumnValue.fromBoolean(i % 2 == 1));
            PutRowRequest request = new PutRowRequest(change);
            otsClient.putRow(request);
        }
    }

    void clear() {
        dropTableIfExists();
    }

    private void dropTableIfExists() {
        try {
            DeleteTableRequest request = new DeleteTableRequest(tableName);
            otsClient.deleteTable(request);
        } catch (TableStoreException e) {
            if (!e.getMessage().equals("Requested table does not exist.")) {
                throw e;
            }
        }
    }
}
