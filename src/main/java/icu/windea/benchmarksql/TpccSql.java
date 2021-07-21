/*
 * Copyright (C) 2021, DragonKnightOfBreeze
 */

package icu.windea.benchmarksql;

import org.apache.log4j.*;

import java.io.*;
import java.util.*;

public final class TpccSql {
    private static final Logger logger = Logger.getLogger(TpccSql.class);

    private static final Map<String, String> sqlMap = new HashMap<>();

    public static String loadConfigSql() {
        return getSql("loadConfig");
    }

    public static String loadItemSql() {
        return getSql("loadItem");
    }

    public static String loadWarehouseSql() {
        return getSql("loadWarehouse");
    }

    public static String loadStockSql() {
        return getSql("loadStock");
    }

    public static String loadDistrictSql() {
        return getSql("loadDistrict");
    }

    public static String loadCustomerSql() {
        return getSql("loadCustomer");
    }

    public static String loadHistorySql() {
        return getSql("loadHistory");
    }

    public static String loadOrderSql() {
        return getSql("loadOrder");
    }

    public static String loadOrderLineSql() {
        return getSql("loadOrderLine");
    }

    public static String loadNewOrderSql() {
        return getSql("loadNewOrder");
    }

    static {
        String[] sqlName = new String[]{""};
        List<String> sqlLines = new ArrayList<>();
        BufferedReader loadDataReader = null;
        BufferedReader benchmarkReader = null;

        try {
            loadDataReader = new BufferedReader(new FileReader(TpccUtil.getSysProp("loadDataFile", null)));
            loadSql(sqlName, sqlLines, loadDataReader);

            benchmarkReader = new BufferedReader(new FileReader(TpccUtil.getSysProp("tpccFile", null)));
            loadSql(sqlName, sqlLines, loadDataReader);
        } catch(Exception e) {
            logger.error("Cannot load internal sql files.", e);
            System.exit(1);
        } finally {
            TpccUtil.closeQuietly(loadDataReader);
            TpccUtil.closeQuietly(benchmarkReader);
        }
    }

    private static void loadSql(String[] sqlName, List<String> sqlLines, BufferedReader loadSqlReader) {
        loadSqlReader.lines().forEach(line -> {
            if(line.startsWith("--")) {
                String text = line.substring(2).trim();
                if(text.startsWith("#")) {
                    saveSql(sqlName, sqlLines);

                    String name = text.substring(1);
                    sqlName[0] = name;
                    sqlLines.clear();
                }
            } else if(!line.trim().isEmpty()) {
                sqlLines.add(line);
            }
        });

        saveSql(sqlName, sqlLines);
    }

    private static void saveSql(String[] sqlName, List<String> sqlLines) {
        if(!sqlName[0].isEmpty()) {
            String sql = String.join("\n", sqlLines).trim();
            if(!sql.isEmpty()) {
                sqlMap.put(sqlName[0], sql);
            }
        }
    }

    private static String getSql(String name) {
        String result = sqlMap.get(name);
        if(result == null) {
            logger.error("Cannot load internal sql '" + name + "'.");
            System.exit(1);
        }
        return result;
    }
}
