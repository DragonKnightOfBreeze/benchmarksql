/*
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 * Copyright (C) 2021, DragonKnightOfBreeze
 */

package icu.windea.benchmarksql;

import java.text.*;

/**
 * Basic configuration parameters for Tpcc
 */
public interface TpccConfig {
    String TPCC_VERSION = "5.0";

    int DB_UNKNOWN = 0;
    int DB_FIREBIRD = 1;
    int DB_ORACLE = 2;
    int DB_POSTGRES = 3;
    
    //int NEW_ORDER = 1;
    //int PAYMENT = 2;
    //int ORDER_STATUS = 3;
    //int DELIVERY = 4;
    //int STOCK_LEVEL = 5;

    //String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //int configCommitCount = 10000;  // commit every n records in LoadData

    //int configWhseCount = 10;
    //int configItemCount = 100000; // tpc-c std = 100,000
    //int configDistPerWhse = 10;     // tpc-c std = 10
    //int configCustPerDist = 3000;   // tpc-c std = 3,000
}
