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

    public static final String loadConfig = getSql("loadConfig");
    public static final String loadItem = getSql("loadItem");
    public static final String loadWarehouse = getSql("loadWarehouse");
    public static final String loadStock = getSql("loadStock");
    public static final String loadDistrict = getSql("loadDistrict");
    public static final String loadCustomer = getSql("loadCustomer");
    public static final String loadHistory = getSql("loadHistory");
    public static final String loadOrder = getSql("loadOrder");
    public static final String loadOrderLine = getSql("loadOrderLine");
    public static final String loadNewOrder = getSql("loadNewOrder");
    
    public static final String newOrderSelectWhseCust = getSql("newOrderSelectWhseCust");
    public static final String newOrderSelectDist = getSql("newOrderSelectDist");
    public static final String newOrderUpdateDist = getSql("newOrderUpdateDist");
    public static final String mewOrderInsertOrder = getSql("newOrderInsertOrder");
    public static final String newOrderInsertNewOrder = getSql("newOrderInsertNewOrder");
    public static final String newOrderSelectStock = getSql("newOrderSelectStock");
    public static final String newOrderSelectItem = getSql("newOrderSelectItem");
    public static final String newOrderUpdateStock = getSql("newOrderUpdateStock");
    public static final String newOrderInsertOrderLine = getSql("newOrderInsertOrderLine");
    public static final String paymentSelectWarehouse = getSql("paymentSelectWarehouse");
    public static final String paymentSelectDistrict = getSql("paymentSelectDistrict");
    public static final String paymentSelectCustomerListByLast = getSql("paymentSelectCustomerListByLast");
    public static final String paymentSelectCustomer = getSql("paymentSelectCustomer");
    public static final String paymentSelectCustomerData = getSql("paymentSelectCustomerData");
    public static final String paymentUpdateWarehouse = getSql("paymentUpdateWarehouse");
    public static final String paymentUpdateDistrict = getSql("paymentUpdateDistrict");
    public static final String paymentUpdateCustomer = getSql("paymentUpdateCustomer");
    public static final String paymentUpdateCustomerWithData = getSql("paymentUpdateCustomerWithData");
    public static final String paymentInsertHistory = getSql("paymentInsertHistory");
    public static final String orderStatusSelectCustomerListByLast = getSql("orderStatusSelectCustomerListByLast");
    public static final String orderStatusSelectCustomer = getSql("orderStatusSelectCustomer");
    public static final String orderStatusSelectLastOrder = getSql("orderStatusSelectLastOrder");
    public static final String orderStatusSelectOrderLine = getSql("orderStatusSelectOrderLine");
    public static final String stockLevelSelectLow = getSql("stockLevelSelectLow");
    public static final String deliveryBGSelectOldestNewOrder = getSql("deliveryBGSelectOldestNewOrder");
    public static final String deliveryBGDeleteOldestNewOrder = getSql("deliveryBGDeleteOldestNewOrder");
    public static final String deliveryBGSelectOrder = getSql("deliveryBGSelectOrder");
    public static final String deliveryBGUpdateOrder = getSql("deliveryBGUpdateOrder");
    public static final String deliveryBGSelectSumOLAmount = getSql("deliveryBGSelectSumOLAmount");
    public static final String deliveryBGUpdateOrderLine = getSql("deliveryBGUpdateOrderLine");
    public static String deliveryBGUpdateCustomer = getSql("deliveryBGUpdateCustomer");
}
