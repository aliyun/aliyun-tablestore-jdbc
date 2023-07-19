package com.alicloud.openservices.tablestore.jdbc;


import java.sql.SQLType;
import java.sql.Types;

public enum OTSType implements SQLType {
    BOOLEAN("BOOLEAN", Types.BOOLEAN, Boolean.class, false, 3L, ""),
    DOUBLE("DOUBLE", Types.DOUBLE, Double.class, true, 22L, "[(M,D)] [UNSIGNED] [ZEROFILL]"),
    BIGINT("BIGINT", Types.BIGINT, Long.class, true, 19L, "[(M)] [UNSIGNED] [ZEROFILL]"),
    VARCHAR("VARCHAR", Types.VARCHAR, String.class, false, 1024, "(M) [CHARACTER SET charset_name] [COLLATE collation_name]"),
    VARBINARY("VARBINARY", Types.VARBINARY, null, false, 1024, "(M)"),
    MEDIUMBLOB("MEDIUMBLOB", Types.LONGVARBINARY, null, false, 2097152, ""),
    MEDIUMTEXT("MEDIUMTEXT", Types.LONGVARCHAR, String.class, false, 2097152, " [CHARACTER SET charset_name] [COLLATE collation_name]"),
    UNKNOWN("UNKNOWN", Types.OTHER, null, false, 65535L, "");

    private final Class<?> javaClass;
    private final String name;
    private final boolean isDecimal;
    private final String createParams;
    private final int jdbcType;
    private long precision;

    OTSType(String mysqlTypeName, int jdbcType, Class<?> javaClass, boolean isDec, long precision, String createParams) {
        this.name = mysqlTypeName;
        this.jdbcType = jdbcType;
        this.javaClass = javaClass;
        this.isDecimal = isDec;
        this.precision = precision;
        this.createParams = createParams;
    }

    public static OTSType getByName(String fullMysqlTypeName) {
        String typeName = "";
        long precision = 0;
        if (fullMysqlTypeName.contains("(")) {
            int paramBeginIndex = fullMysqlTypeName.indexOf("(");
            int paramEndIndex = fullMysqlTypeName.indexOf(")");
            typeName = fullMysqlTypeName.substring(0, paramBeginIndex).trim();
            precision = Integer.parseInt(fullMysqlTypeName.substring(paramBeginIndex + 1, paramEndIndex));
        } else {
            typeName = fullMysqlTypeName;
        }
        OTSType type = UNKNOWN;
        String typeNameUpperCase = typeName.toUpperCase();
        if (typeNameUpperCase.equals("BIGINT")) {
            type = BIGINT;
        } else if (typeNameUpperCase.equals("DOUBLE")) {
            type = DOUBLE;
        } else if (typeNameUpperCase.equals("VARBINARY")) {
            type = VARBINARY;
        } else if (typeNameUpperCase.equals("VARCHAR")) {
            type = VARCHAR;
        } else if (typeNameUpperCase.equals("MEDIUMBLOB")) {
            type = MEDIUMBLOB;
        } else if (typeNameUpperCase.equals("MEDIUMTEXT")) {
            type = MEDIUMTEXT;
        } else if (typeNameUpperCase.equals("TINYINT")) {
            type = BOOLEAN;
        }
        if (precision > 0) {
            type.precision = precision;
        }
        return type;
    }

    public static SQLType getByJdbcType(int jdbcType) {
        switch (jdbcType) {
            case Types.VARCHAR:
                return VARCHAR;
            case Types.BIGINT:
                return BIGINT;
            case Types.VARBINARY:
                return VARBINARY;
            case Types.DOUBLE:
                return DOUBLE;
            case Types.BOOLEAN:
                return BOOLEAN;
            default:
                return UNKNOWN;
        }
    }

    public static boolean isSigned(OTSType type) {
        switch (type) {
            case BIGINT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    public static boolean isLiteralL(OTSType type) {
        switch (type) {
            case VARBINARY:
            case VARCHAR:
            case MEDIUMBLOB:
            case MEDIUMTEXT:
                return true;
            default:
                return false;
        }
    }

    public String getName() {
        return this.name;
    }

    public int getJdbcType() {
        return this.jdbcType;
    }

    public String getClassName() {
        return this.javaClass == null ? "[B" : this.javaClass.getName();
    }

    public boolean isDecimal() {
        return this.isDecimal;
    }

    public long getPrecision() {
        return this.precision;
    }

    public String getCreateParams() {
        return this.createParams;
    }

    public String getVendor() {
        return "com.alicloud.openservices.tablestore.jdbc";
    }

    public Integer getVendorTypeNumber() {
        return this.jdbcType;
    }
}
