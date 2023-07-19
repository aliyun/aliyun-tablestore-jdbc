package com.alicloud.openservices.tablestore.jdbc;

public class StringUtils {

    public static String quoteIdentifier(String identifier, String quoteChar) {
        if (identifier == null) {
            return null;
        }
        int quoteCharLength = quoteChar.length();
        if (quoteCharLength == 0) {
            return identifier;
        }
        return quoteChar + identifier.replaceAll(quoteChar, quoteChar + quoteChar) + quoteChar;
    }
}
