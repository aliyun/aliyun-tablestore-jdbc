package com.alicloud.openservices.tablestore.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

public class OTSDatabaseMetaDataTest {

    @BeforeClass
    public static void loadDriver() throws ClassNotFoundException {
        Class.forName(OTSDriver.class.getName());
    }

    @Test
    public void testEmptyResultSets() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:ots:https://access_key_id:access_key_secret@example.com/instance_name");
        DatabaseMetaData meta = connection.getMetaData();
        Assert.assertNotNull(meta.getProcedures(null, null, null));
        Assert.assertNotNull(meta.getProcedureColumns(null, null, null, null));
        Assert.assertNotNull(meta.getColumnPrivileges(null, null, null, null));
        Assert.assertNotNull(meta.getTablePrivileges(null, null, null));
        Assert.assertNotNull(meta.getBestRowIdentifier(null, null, null, 0, false));
        Assert.assertNotNull(meta.getVersionColumns(null, null, null));
        Assert.assertNotNull(meta.getImportedKeys(null, null, null));
        Assert.assertNotNull(meta.getExportedKeys(null, null, null));
        Assert.assertNotNull(meta.getCrossReference(null, null, null, null, null, null));
        Assert.assertNotNull(meta.getUDTs(null, null, null, null));
        Assert.assertNotNull(meta.getSuperTypes(null, null, null));
        Assert.assertNotNull(meta.getSuperTables(null, null, null));
        Assert.assertNotNull(meta.getAttributes(null, null, null, null));
        Assert.assertNotNull(meta.getClientInfoProperties());
        Assert.assertNotNull(meta.getFunctions(null, null, null));
        Assert.assertNotNull(meta.getFunctionColumns(null, null, null, null));
        Assert.assertNotNull(meta.getPseudoColumns(null, null, null, null));
    }
}
