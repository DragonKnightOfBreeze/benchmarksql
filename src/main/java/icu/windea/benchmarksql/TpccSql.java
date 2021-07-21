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

    static {
        String[] sqlName = new String[1];
        List<String> sqlLines = new ArrayList<>();
        InputStream loadDataInputStream = null;
        BufferedReader loadDataReader = null;
        InputStream benchmarkInputStream = null;
        BufferedReader benchmarkReader = null;

        try {
            loadDataInputStream = TpccSql.class.getResourceAsStream("/run/sql.internal/loadData.sql");
            if(loadDataInputStream == null) {
                logger.error("Cannot load internal sql from file 'run/sql.internal/loadData.sql'.");
                System.exit(1);
            }
            loadDataReader = new BufferedReader(new InputStreamReader(loadDataInputStream));
            loadSqlFromFile(sqlName, sqlLines,loadDataReader);

            benchmarkInputStream = TpccSql.class.getResourceAsStream("/run/sql.internal/benchmark.sql");
            if(benchmarkInputStream == null) {
                logger.error("Cannot load internal sql from file 'run/sql.internal/benchmark.sql'.");
                System.exit(1);
            }
            benchmarkReader = new BufferedReader(new InputStreamReader(benchmarkInputStream));
            loadSqlFromFile(sqlName, sqlLines,loadDataReader);
        } catch(Exception e) {
            logger.error("Cannot load internal sql from directory 'run/sql.internal'.", e);
            System.exit(1);
        }finally {
            TpccUtil.closeQuietly(loadDataInputStream);
            TpccUtil.closeQuietly(loadDataReader);
            TpccUtil.closeQuietly(benchmarkInputStream);
            TpccUtil.closeQuietly(benchmarkReader);
        }
    }

    private static void loadSqlFromFile(String[] sqlName,List<String> sqlLines, BufferedReader loadSqlReader) {
        loadSqlReader.lines().forEach(line->{
            if(line.startsWith("--")){
                String text = line.substring(2).trim();
                if(text.startsWith("#")){
                    if(!sqlName[0].isEmpty() && !sqlLines.isEmpty()){
                        sqlMap.put(sqlName[0],String.join("\n", sqlLines));
                    }
                    
                    String name = text.substring(1);
                    sqlName[0] = name;
                    sqlLines.clear();
                }
            }else if(!line.trim().isEmpty()){
                sqlLines.add(line);
            }
        });
    }
    
    public static String loadConfigSql() {
        return sqlMap.get("loadConfig");
    }

    public static String loadItemSql() {
        return sqlMap.get("loadItem");
    }

    public static String loadWarehouseSql() {
        return sqlMap.get("loadWarehouse");
    }

    public static String loadStockSql() {
        return sqlMap.get("loadStock");
    }

    public static String loadDistrictSql() {
        return sqlMap.get("loadDistrict");
    }

    public static String loadCustomerSql() {
        return sqlMap.get("loadCustomer");
    }

    public static String loadHistorySql() {
        return sqlMap.get("loadHistory");
    }

    public static String loadOrderSql() {
        return sqlMap.get("loadOrder");
    }

    public static String loadOrderLineSql() {
        return sqlMap.get("loadOrderLine");
    }

    public static String loadNewOrderSql() {
        return sqlMap.get("loadNewOrder");
    }
}
