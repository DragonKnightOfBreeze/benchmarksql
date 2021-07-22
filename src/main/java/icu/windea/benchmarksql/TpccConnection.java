/*
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 * Copyright (C) 2021, DragonKnightOfBreeze
 */

package icu.windea.benchmarksql;

import java.sql.*;
import java.util.*;

/**
 * One connection to the database. Used by either the old style Terminal or the new TimedSUT.
 */
public final class TpccConnection {
    public final PreparedStatement stmtNewOrderSelectWhseCust;
    public final PreparedStatement stmtNewOrderSelectDist;
    public final PreparedStatement stmtNewOrderUpdateDist;
    public final PreparedStatement stmtNewOrderInsertOrder;
    public final PreparedStatement stmtNewOrderInsertNewOrder;
    public final PreparedStatement stmtNewOrderSelectStock;
    public final PreparedStatement stmtNewOrderSelectItem;
    public final PreparedStatement stmtNewOrderUpdateStock;
    public final PreparedStatement stmtNewOrderInsertOrderLine;
    public final PreparedStatement stmtPaymentSelectWarehouse;
    public final PreparedStatement stmtPaymentSelectDistrict;
    public final PreparedStatement stmtPaymentSelectCustomerListByLast;
    public final PreparedStatement stmtPaymentSelectCustomer;
    public final PreparedStatement stmtPaymentSelectCustomerData;
    public final PreparedStatement stmtPaymentUpdateWarehouse;
    public final PreparedStatement stmtPaymentUpdateDistrict;
    public final PreparedStatement stmtPaymentUpdateCustomer;
    public final PreparedStatement stmtPaymentUpdateCustomerWithData;
    public final PreparedStatement stmtPaymentInsertHistory;
    public final PreparedStatement stmtOrderStatusSelectCustomerListByLast;
    public final PreparedStatement stmtOrderStatusSelectCustomer;
    public final PreparedStatement stmtOrderStatusSelectLastOrder;
    public final PreparedStatement stmtOrderStatusSelectOrderLine;
    public final PreparedStatement stmtStockLevelSelectLow;
    public final PreparedStatement stmtDeliveryBGSelectOldestNewOrder;
    public final PreparedStatement stmtDeliveryBGDeleteOldestNewOrder;
    public final PreparedStatement stmtDeliveryBGSelectOrder;
    public final PreparedStatement stmtDeliveryBGUpdateOrder;
    public final PreparedStatement stmtDeliveryBGSelectSumOLAmount;
    public final PreparedStatement stmtDeliveryBGUpdateOrderLine;
    public final PreparedStatement stmtDeliveryBGUpdateCustomer;
    private final Connection dbConn;
    private final int dbType;

    public TpccConnection(Connection dbConn, int dbType) throws SQLException {
        this.dbConn = dbConn;
        this.dbType = dbType;

        // PreparedStatements for NEW_ORDER
        stmtNewOrderSelectWhseCust = dbConn.prepareStatement(TpccSql.newOrderSelectWhseCust);
        stmtNewOrderSelectDist = dbConn.prepareStatement(TpccSql.newOrderSelectDist);
        stmtNewOrderUpdateDist = dbConn.prepareStatement(TpccSql.newOrderUpdateDist);
        stmtNewOrderInsertOrder = dbConn.prepareStatement(TpccSql.mewOrderInsertOrder);
        stmtNewOrderInsertNewOrder = dbConn.prepareStatement(TpccSql.newOrderInsertNewOrder);
        stmtNewOrderSelectStock = dbConn.prepareStatement(TpccSql.newOrderSelectStock);
        stmtNewOrderSelectItem = dbConn.prepareStatement(TpccSql.newOrderSelectItem);
        stmtNewOrderUpdateStock = dbConn.prepareStatement(TpccSql.newOrderUpdateStock);
        stmtNewOrderInsertOrderLine = dbConn.prepareStatement(TpccSql.newOrderInsertOrderLine);

        // PreparedStatements for PAYMENT
        stmtPaymentSelectWarehouse = dbConn.prepareStatement(TpccSql.paymentSelectWarehouse);
        stmtPaymentSelectDistrict = dbConn.prepareStatement(TpccSql.paymentSelectDistrict);
        stmtPaymentSelectCustomerListByLast = dbConn.prepareStatement(TpccSql.paymentSelectCustomerListByLast);
        stmtPaymentSelectCustomer = dbConn.prepareStatement(TpccSql.paymentSelectCustomer);
        stmtPaymentSelectCustomerData = dbConn.prepareStatement(TpccSql.paymentSelectCustomerData);
        stmtPaymentUpdateWarehouse = dbConn.prepareStatement(TpccSql.paymentUpdateWarehouse);
        stmtPaymentUpdateDistrict = dbConn.prepareStatement(TpccSql.paymentUpdateDistrict);
        stmtPaymentUpdateCustomer = dbConn.prepareStatement(TpccSql.paymentUpdateCustomer);
        stmtPaymentUpdateCustomerWithData = dbConn.prepareStatement(TpccSql.paymentUpdateCustomerWithData);
        stmtPaymentInsertHistory = dbConn.prepareStatement(TpccSql.paymentInsertHistory);

        // PreparedStatements for ORDER_STATUS
        stmtOrderStatusSelectCustomerListByLast = dbConn.prepareStatement(TpccSql.orderStatusSelectCustomerListByLast);
        stmtOrderStatusSelectCustomer = dbConn.prepareStatement(TpccSql.orderStatusSelectCustomer);
        stmtOrderStatusSelectLastOrder = dbConn.prepareStatement(TpccSql.orderStatusSelectLastOrder);
        stmtOrderStatusSelectOrderLine = dbConn.prepareStatement(TpccSql.orderStatusSelectOrderLine);

        // PreparedStatements for STOCK_LEVEL
        stmtStockLevelSelectLow = dbConn.prepareStatement(TpccSql.stockLevelSelectLow);
        // PreparedStatements for DELIVERY_BG
        stmtDeliveryBGSelectOldestNewOrder = dbConn.prepareStatement(TpccSql.deliveryBGSelectOldestNewOrder);
        stmtDeliveryBGDeleteOldestNewOrder = dbConn.prepareStatement(TpccSql.deliveryBGDeleteOldestNewOrder);
        stmtDeliveryBGSelectOrder = dbConn.prepareStatement(TpccSql.deliveryBGSelectOrder);
        stmtDeliveryBGUpdateOrder = dbConn.prepareStatement(TpccSql.deliveryBGUpdateOrder);
        stmtDeliveryBGSelectSumOLAmount = dbConn.prepareStatement(TpccSql.deliveryBGSelectSumOLAmount);
        stmtDeliveryBGUpdateOrderLine = dbConn.prepareStatement(TpccSql.deliveryBGUpdateOrderLine);
        stmtDeliveryBGUpdateCustomer = dbConn.prepareStatement(TpccSql.deliveryBGUpdateCustomer);
    }

    public TpccConnection(String connURL, Properties connProps, int dbType) throws SQLException {
        this(DriverManager.getConnection(connURL, connProps), dbType);
    }

    public void commit() throws SQLException {
        dbConn.commit();
    }

    public void rollback() throws SQLException {
        dbConn.rollback();
    }
}
