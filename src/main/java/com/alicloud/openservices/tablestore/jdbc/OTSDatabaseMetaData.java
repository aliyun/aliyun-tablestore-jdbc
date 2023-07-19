package com.alicloud.openservices.tablestore.jdbc;

import com.alicloud.openservices.tablestore.model.ColumnType;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OTSDatabaseMetaData extends WrapperAdapter implements DatabaseMetaData {

    static final int MAX_STRING_LENGTH = 2 * 1024 * 1024;
    static final int MAX_BINARY_LENGTH = 2 * 1024 * 1024;
    static final String quoteId = "`";
    static final String quoteString = "'";
    private static final String OTS_PRODUCT_NAME = "TableStore";
    private static final String OTS_DRIVER_NAME = "tablestore-jdbc";
    private static final String CATALOG_TERM = "instance";
    private static final int MAX_CATALOG_NAME_LENGTH = 16;
    private static final int MAX_TABLE_NAME_LENGTH = 255;
    private final OTSConnection connection;

    OTSDatabaseMetaData(OTSConnection conn) {
        this.connection = conn;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.config.getEndPoint();
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.config.getAccessKeyId();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return OTS_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return OTSDriver.OTS_DRIVER_VERSION;
    }

    @Override
    public String getDriverName() throws SQLException {
        return OTS_DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return OTSDriver.OTS_DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return OTSDriver.OTS_DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getDriverMinorVersion() {
        return OTSDriver.OTS_DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return quoteString;
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "ACCESSIBLE,ADD,ANALYZE,ASC,BEFORE,CASCADE,CHANGE,CONTINUE,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND" +
                ",DAY_MINUTE,DAY_SECOND,DELAYED,DESC,DISTINCTROW,DIV,DUAL,ELSEIF,EMPTY,ENCLOSED,ESCAPED,EXIT,EXPLAIN," +
                "FIRST_VALUE,FLOAT4,FLOAT8,FORCE,FULLTEXT,GENERATED,GROUPS,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE," +
                "HOUR_SECOND,IF,IGNORE,INDEX,INFILE,INT1,INT2,INT3,INT4,INT8,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,ITERATE," +
                "JSON_TABLE,KEY,KEYS,KILL,LAG,LAST_VALUE,LEAD,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCK,LONG,LONGBLOB,LONGTEXT," +
                "LOOP,LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT," +
                "MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,NO_WRITE_TO_BINLOG,NTH_VALUE,NTILE,OPTIMIZE,OPTIMIZER_COSTS," +
                "OPTION,OPTIONALLY,OUTFILE,PURGE,READ,READ_WRITE,REGEXP,RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT," +
                "RLIKE,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SEPARATOR,SHOW,SIGNAL,SPATIAL,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS," +
                "SQL_SMALL_RESULT,SSL,STARTING,STORED,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,UNDO,UNLOCK," +
                "UNSIGNED,USAGE,USE,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,VIRTUAL,WHILE,WRITE,XOR," +
                "YEAR_MONTH,ZEROFILL";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "ASCII,BIN,BIT_LENGTH,CHAR,CHARACTER_LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,CONV,ELT,EXPORT_SET,FIELD," +
                "FIND_IN_SET,HEX,INSERT,INSTR,LCASE,LEFT,LENGTH,LOAD_FILE,LOCATE,LOCATE,LOWER,LPAD,LTRIM,MAKE_SET," +
                "MATCH,MID,OCT,OCTET_LENGTH,ORD,POSITION,QUOTE,REPEAT,REPLACE,REVERSE,RIGHT,RPAD,RTRIM,SOUNDEX,SPACE" +
                ",STRCMP,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING,SUBSTRING_INDEX,TRIM,UCASE,UPPER";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "#@";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return CATALOG_TERM;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return MAX_CATALOG_NAME_LENGTH;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return MAX_TABLE_NAME_LENGTH;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "RESERVERD", "RESERVERD", "RESERVERD", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING));
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("STUPID_PLACEHOLDERS", "USELESS_PLACEHOLDER"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING));
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getTables(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String[] types) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT",
                        "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.STRING));
        List<Object[]> rows = new ArrayList<>();
        // check types
        if (types != null) {
            boolean hasTableType = false;
            for (String type : types) {
                if (type.equals("TABLE")) {
                    hasTableType = true;
                    break;
                }
            }
            if (!hasTableType) {
                return new OTSResultSet(meta, rows);
            }
        }
        // show tables
        Statement statement = connection.createStatement();
        if (tableNamePattern == null) {
            statement.executeQuery(String.format("SHOW TABLES FROM %s",
                    StringUtils.quoteIdentifier(catalog, quoteId)));
        } else {
            statement.executeQuery(String.format("SHOW TABLES FROM %s LIKE %s",
                    StringUtils.quoteIdentifier(catalog, quoteId),
                    StringUtils.quoteIdentifier(tableNamePattern, quoteString)));
        }
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            Object[] row = new Object[10];
            row[0] = catalog;
            row[2] = resultSet.getString(1);
            row[3] = "TABLE";
            rows.add(row);
        }
        return new OTSResultSet(meta, rows);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING));
        List<Object[]> rows = new ArrayList<>();
        return new OTSResultSet(meta, rows);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Collections.singletonList("TABLE_CAT"),
                Collections.singletonList(ColumnType.STRING));
        List<Object[]> rows = new ArrayList<>();
        rows.add(new String[]{connection.config.getInstanceName()});
        return new OTSResultSet(meta, rows);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Collections.singletonList("TABLE_TYPE"),
                Collections.singletonList(ColumnType.STRING));
        List<Object[]> rows = new ArrayList<>();
        rows.add(new String[]{"TABLE"});
        return new OTSResultSet(meta, rows);
    }

    @Override
    public ResultSet getColumns(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PERC_RADIX", "NULLABLE", "REMARKS",
                        "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
                        "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
                        "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.INTEGER, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING));
        List<Object[]> rows = new ArrayList<>();
        // show tables
        ResultSet tables = getTables(catalog, schemaPattern, tableNamePattern, null);
        while (tables.next()) {
            // show columns
            String tableName = tables.getString("TABLE_NAME");
            Statement statement = connection.createStatement();
            if (columnNamePattern != null) {
                statement.executeQuery(String.format("SHOW FULL COLUMNS FROM %s FROM %s LIKE %s",
                        StringUtils.quoteIdentifier(tableName, quoteId),
                        StringUtils.quoteIdentifier(catalog, quoteId),
                        StringUtils.quoteIdentifier(columnNamePattern, quoteString)));
            } else {
                statement.executeQuery(String.format("SHOW FULL COLUMNS FROM %s FROM %s",
                        StringUtils.quoteIdentifier(tableName, quoteId),
                        StringUtils.quoteIdentifier(catalog, quoteId)));
            }
            ResultSet resultSet = statement.getResultSet();
            long position = 1;
            while (resultSet.next()) {
                OTSType type = OTSType.getByName(resultSet.getString("Type"));
                String isNullable = resultSet.getString("Null");
                Object[] row = new Object[24];
                row[0] = catalog;
                row[2] = tableName;
                row[3] = resultSet.getString("Field");
                row[4] = (long) type.getJdbcType();
                row[5] = type.getName();
                row[6] = type.getPrecision();
                row[9] = 10L;
                if (isNullable.equals("YES")) {
                    row[10] = (long) columnNullable;
                } else if (isNullable.equals("NO")) {
                    row[10] = (long) columnNoNulls;
                } else {
                    row[10] = (long) columnNullableUnknown;
                }
                row[16] = position++;
                row[17] = isNullable;
                row[22] = "NO";
                row[23] = "NO";
                rows.add(row);
            }
        }
        return new OTSResultSet(meta, rows);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
                Arrays.asList(ColumnType.INTEGER, ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "PSEUDO_COLUMN"),
                Arrays.asList(ColumnType.INTEGER, ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING));
        List<Object[]> rows = new ArrayList<>();
        // show keys
        Statement statement = connection.createStatement();
        statement.executeQuery(String.format("SHOW KEYS FROM %s FROM %s",
                StringUtils.quoteIdentifier(table, quoteId),
                StringUtils.quoteIdentifier(catalog, quoteId)));
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            String keyName = resultSet.getString("Key_name");
            if (!keyName.equals("PRIMARY")) {
                continue;
            }
            Object[] row = new Object[6];
            row[0] = catalog;
            row[2] = resultSet.getString("Table");
            row[3] = resultSet.getString("Column_name");
            row[4] = resultSet.getLong("Seq_in_index");
            row[5] = keyName;
            rows.add(row);
        }
        return new OTSResultSet(meta, rows);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                        "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                        "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
                        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE",
                        "DELETE_RULE", "FK_NAME", "PK_NAME", "DEFERRABILITY"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS",
                        "NULLABLE", "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE",
                        "AUTO_INCREMENT", "LOCAL_TYPE_NAME", "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE",
                        "SQL_DATETIME_SUB", "NUM_PREC_RADIX"),
                Arrays.asList(ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.BOOLEAN, ColumnType.INTEGER,
                        ColumnType.BOOLEAN, ColumnType.BOOLEAN, ColumnType.BOOLEAN, ColumnType.STRING, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER));
        List<Object[]> rows = new ArrayList<>();
        OTSType[] types = new OTSType[]{
                OTSType.BIGINT, OTSType.DOUBLE, OTSType.BOOLEAN, OTSType.VARCHAR,
                OTSType.VARBINARY, OTSType.MEDIUMBLOB, OTSType.MEDIUMTEXT
        };
        for (OTSType type : types) {
            Object[] row = new Object[18];
            row[0] = type.getName();
            row[1] = type.getJdbcType();
            row[2] = type.getPrecision();
            if (OTSType.isLiteralL(type)) {
                row[3] = quoteString;
                row[4] = quoteString;
            }
            row[5] = type.getCreateParams();
            row[6] = typeNullable;
            row[7] = true;
            row[8] = typeSearchable;
            row[9] = !OTSType.isSigned(type);
            row[10] = false;
            row[11] = false;
            row[12] = type.getName();
            row[17] = 10;
            rows.add(row);
        }
        return new OTSResultSet(meta, rows);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
                        "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.BOOLEAN,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.STRING,
                        ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.STRING));
        List<Object[]> rows = new ArrayList<>();
        // show index
        Statement statement = connection.createStatement();
        statement.executeQuery(String.format("SHOW INDEX IN %s FROM %s",
                StringUtils.quoteIdentifier(table, quoteId),
                StringUtils.quoteIdentifier(catalog, quoteId)));
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            String isDefinedColumn = resultSet.getString("Is_defined_column");
            if (isDefinedColumn != null && isDefinedColumn.equals("YES")) {
                continue;
            }
            Object[] row = new Object[13];
            row[0] = catalog;
            row[2] = table;
            row[3] = resultSet.getBoolean("Non_unique");
            row[5] = resultSet.getString("Key_name");
            row[6] = (long) tableIndexOther;
            row[7] = resultSet.getLong("Seq_in_index");
            row[8] = resultSet.getString("Column_name");
            row[9] = "A";
            row[10] = resultSet.getLong("Cardinality");
            row[11] = 0L;
            rows.add(row);
        }
        return new OTSResultSet(meta, rows);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING, ColumnType.INTEGER)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public OTSConnection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME",
                        "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF",
                        "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                        "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER,
                        ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return OTSDriver.OTS_DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return OTSDriver.OTS_DRIVER_MINOR_VERSION;
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return OTSDriver.OTS_JDBC_MAJOR_VERSION;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return OTSDriver.OTS_JDBC_MINOR_VERSION;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION"),
                Arrays.asList(ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE",
                        "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS",
                        "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.STRING, ColumnType.INTEGER,
                        ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.STRING, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.STRING, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        OTSResultSetMetaData meta = new OTSResultSetMetaData(
                Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE",
                        "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE"),
                Arrays.asList(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING,
                        ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER, ColumnType.INTEGER,
                        ColumnType.STRING, ColumnType.STRING, ColumnType.INTEGER, ColumnType.STRING)
        );
        return new OTSResultSet(meta, new ArrayList<>());
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
