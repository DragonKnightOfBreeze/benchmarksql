/*
 * Copyright (C) 2003, Raul Barbosa
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 * Copyright (C) 2021, DragonKnightOfBreeze
 */

package icu.windea.benchmarksql;

import org.apache.log4j.*;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.regex.*;

/**
 * Open Source Java implementation of a TPC-C like benchmark.
 */
public final class Tpcc implements TpccConfig {
    private static final Logger logger = Logger.getLogger(Tpcc.class);
    
    private static String resultDirName = null;
    private static BufferedWriter resultCSV = null;
    private static BufferedWriter runInfoCSV = null;
    private static int runID = 0;

    private int dbType = DB_UNKNOWN;
    private int currentlyDisplayedTerminal;

    private TpccTerminal[] terminals;
    private String[] terminalNames;
    private boolean terminalsBlockingExit = false;
    private long terminalsStarted = 0;
    private final long sessionCount = 0;
    private long transactionCount = 0;
    private final Object counterLock = new Object();

    private long newOrderCounter = 0;
    private long sessionStartTimestamp;
    private long sessionEndTimestamp;
    private long sessionNextTimestamp = 0;
    private long sessionNextKounter = 0;

    private long sessionEndTargetTime = -1;
    private long fastNewOrderCounter;
    private long recentTpmC = 0;
    private long recentTpmTotal = 0;

    private boolean signalTerminalsRequestEndSent = false;
    private boolean databaseDriverLoaded = false;

    private FileOutputStream fileOutputStream;
    private PrintStream printStreamReport;
    private String sessionStart, sessionEnd;
    private int limPerMin_Terminal;

    private double tpmC;
    private TpccRandom rnd;
    private OsCollector osCollector = null;

    public Tpcc() {
        // load the ini file
        Properties ini = new Properties();
        try {
            ini.load(new FileInputStream(System.getProperty("prop")));
        } catch(IOException e) {
            errorMessage("Term-00, could not load properties file");
        }

        logger.info("Term-00, ");
        logger.info("Term-00, +-------------------------------------------------------------+");
        logger.info("Term-00,      BenchmarkSQL v" + TPCC_VERSION);
        logger.info("Term-00, +-------------------------------------------------------------+");
        logger.info("Term-00,  (c) 2003, Raul Barbosa");
        logger.info("Term-00,  (c) 2004-2016, Denis Lussier");
        logger.info("Term-00,  (c) 2016, Jan Wieck");
        logger.info("Term-00, +-------------------------------------------------------------+");
        logger.info("Term-00, ");
        String iDB = getProp(ini, "db");
        String iDriver = getProp(ini, "driver");
        String iConn = getProp(ini, "conn");
        String iUser = getProp(ini, "user");
        String iPassword = ini.getProperty("password");

        logger.info("Term-00, ");
        String iWarehouses = getProp(ini, "warehouses");
        String iTerminals = getProp(ini, "terminals");

        String iRunTxnsPerTerminal = ini.getProperty("runTxnsPerTerminal");
        String iRunMins = ini.getProperty("runMins");
        if(Integer.parseInt(iRunTxnsPerTerminal) == 0 && Integer.parseInt(iRunMins) != 0) {
            logger.info("Term-00, runMins" + "=" + iRunMins);
        } else if(Integer.parseInt(iRunTxnsPerTerminal) != 0 && Integer.parseInt(iRunMins) == 0) {
            logger.info("Term-00, runTxnsPerTerminal" + "=" + iRunTxnsPerTerminal);
        } else {
            errorMessage("Term-00, Must indicate either transactions per terminal or number of run minutes!");
        }
        String limPerMin = getProp(ini, "limitTxnsPerMin");
        String iTermWhseFixed = getProp(ini, "terminalWarehouseFixed");
        logger.info("Term-00, ");
        String iNewOrderWeight = getProp(ini, "newOrderWeight");
        String iPaymentWeight = getProp(ini, "paymentWeight");
        String iOrderStatusWeight = getProp(ini, "orderStatusWeight");
        String iDeliveryWeight = getProp(ini, "deliveryWeight");
        String iStockLevelWeight = getProp(ini, "stockLevelWeight");

        logger.info("Term-00, ");
        String resultDirectory = getProp(ini, "resultDirectory");
        String osCollectorScript = getProp(ini, "osCollectorScript");

        logger.info("Term-00, ");

        if(iDB.equals("firebird")) {
            dbType = DB_FIREBIRD;
        } else if(iDB.equals("oracle")) {
            dbType = DB_ORACLE;
        } else if(iDB.equals("postgres")) {
            dbType = DB_POSTGRES;
        } else if(iDB.equals("mysql")) {
            dbType = DB_UNKNOWN;
        } else {
            logger.error("unknown database type '" + iDB + "'");
            return;
        }

        if(Integer.parseInt(limPerMin) != 0) {
            limPerMin_Terminal = Integer.parseInt(limPerMin) / Integer.parseInt(iTerminals);
        } else {
            limPerMin_Terminal = -1;
        }

        boolean iRunMinsBool = false;

        try {
            String driver = iDriver;
            printMessage("Loading database driver: '" + driver + "'...");
            Class.forName(iDriver);
            databaseDriverLoaded = true;
        } catch(Exception ex) {
            errorMessage("Unable to load the database driver!");
            databaseDriverLoaded = false;
        }

        if(databaseDriverLoaded && resultDirectory != null) {
            StringBuffer sb = new StringBuffer();
            Formatter fmt = new Formatter(sb);
            Pattern p = Pattern.compile("%t");
            Calendar cal = Calendar.getInstance();

            String iRunID;

            iRunID = System.getProperty("runID");
            if(iRunID != null) {
                runID = Integer.parseInt(iRunID);
            }

            /*
             * Split the resultDirectory into strings around
             * patterns of %t and then insert date/time formatting
             * based on the current time. That way the resultDirectory
             * in the properties file can have date/time format
             * elements like in result_%tY-%tm-%td to embed the current
             * date in the directory name.
             */
            String[] parts = p.split(resultDirectory, -1);
            sb.append(parts[0]);
            for(int i = 1; i < parts.length; i++) {
                fmt.format("%t" + parts[i].charAt(0), cal);
                sb.append(parts[i].substring(1));
            }
            resultDirName = sb.toString();
            File resultDir = new File(resultDirName);
            File resultDataDir = new File(resultDir, "data");

            // Create the output directory structure.
            if(!resultDir.mkdir()) {
                logger.error("Failed to create directory '" +
                    resultDir.getPath() + "'");
                System.exit(1);
            }
            if(!resultDataDir.mkdir()) {
                logger.error("Failed to create directory '" +
                    resultDataDir.getPath() + "'");
                System.exit(1);
            }
    
            // Copy the used properties file into the resultDirectory.
            try {
                Files.copy(new File(System.getProperty("prop")).toPath(),
                    new File(resultDir, "run.properties").toPath());
            } catch(IOException e) {
                logger.error(e.getMessage());
                System.exit(1);
            }
            logger.info("Term-00, copied " + System.getProperty("prop") +
                " to " + new File(resultDir, "run.properties").toPath());

            // Create the runInfo.csv file.
            String runInfoCSVName = new File(resultDataDir, "runInfo.csv").getPath();
            try {
                runInfoCSV = new BufferedWriter(
                    new FileWriter(runInfoCSVName));
                runInfoCSV.write("run,driver,driverVersion,db,sessionStart,runMins," +
                    "loadWarehouses,runWarehouses,numSUTThreads,limitTxnsPerMin," +
                    "thinkTimeMultiplier,keyingTimeMultiplier\n");
            } catch(IOException e) {
                logger.error(e.getMessage());
                System.exit(1);
            }
            logger.info("Term-00, created " + runInfoCSVName + " for runID " +
                runID);

            // Open the per transaction result.csv file.
            String resultCSVName = new File(resultDataDir, "result.csv").getPath();
            try {
                resultCSV = new BufferedWriter(new FileWriter(resultCSVName));
                resultCSV.write("run,elapsed,latency,dblatency,ttype,rbk,dskipped,error\n");
            } catch(IOException e) {
                logger.error(e.getMessage());
                System.exit(1);
            }
            logger.info("Term-00, writing per transaction results to " +
                resultCSVName);

            if(osCollectorScript != null) {
                osCollector = new OsCollector(
                    getProp(ini, "osCollectorScript"), runID, Integer.parseInt(getProp(ini, "osCollectorInterval")), 
                    getProp(ini, "osCollectorSSHAddr"), getProp(ini, "osCollectorDevices"), resultDataDir
                );
            }

            logger.info("Term-00,");
        }

        if(databaseDriverLoaded) {
            boolean limitIsTime = iRunMinsBool;
            int numTerminals = -1;
            int transactionsPerTerminal = -1;
            int numWarehouses = -1;
            int loadWarehouses = -1;
            int newOrderWeightValue = -1;
            int paymentWeightValue = -1;
            int orderStatusWeightValue = -1;
            int deliveryWeightValue = -1;
            int stockLevelWeightValue = -1;
            long executionTimeMillis = -1;
            boolean terminalWarehouseFixed = true;
            long CLoad;
            Properties dbProps = new Properties();

            try {
                dbProps.setProperty("user", iUser);
                dbProps.setProperty("password", iPassword);

                /*
                 * Fine tuning of database connection parameters if needed.
                 */
                switch(dbType) {
                case DB_FIREBIRD:
                    /*
                     * Firebird needs no_rec_version for our load
                     * to work. Even with that some "deadlocks"
                     * occur. Note that the message "deadlock" in
                     * Firebird can mean something completely different,
                     * namely that there was a conflicting write to
                     * a row that could not be resolved.
                     */
                    dbProps.setProperty("TRANSACTION_READ_COMMITTED",
                        "isc_tpb_read_committed," +
                            "isc_tpb_no_rec_version," +
                            "isc_tpb_write," +
                            "isc_tpb_wait");
                    break;
                default:
                    break;
                }

                try {
                    loadWarehouses = Integer.parseInt(TpccUtil.getConfig(iConn, dbProps, "warehouses"));
                    CLoad = Long.parseLong(TpccUtil.getConfig(iConn, dbProps, "nURandCLast"));
                } catch(Exception e) {
                    errorMessage(e.getMessage());
                    throw e;
                }
                this.rnd = new TpccRandom(CLoad);
                logger.info("Term-00, C value for C_LAST during load: " + CLoad);
                logger.info("Term-00, C value for C_LAST this run:    " + rnd.getNURandCLast());
                logger.info("Term-00, ");

                fastNewOrderCounter = 0;
                updateStatusLine();

                try {
                    if(Integer.parseInt(iRunMins) != 0 && Integer.parseInt(iRunTxnsPerTerminal) == 0) {
                        iRunMinsBool = true;
                    } else if(Integer.parseInt(iRunMins) == 0 && Integer.parseInt(iRunTxnsPerTerminal) != 0) {
                        iRunMinsBool = false;
                    } else {
                        throw new NumberFormatException();
                    }
                } catch(NumberFormatException e1) {
                    errorMessage("Must indicate either transactions per terminal or number of run minutes!");
                    throw new Exception();
                }

                try {
                    numWarehouses = Integer.parseInt(iWarehouses);
                    if(numWarehouses <= 0) {
                        throw new NumberFormatException();
                    }
                } catch(NumberFormatException e1) {
                    errorMessage("Invalid number of warehouses!");
                    throw new Exception();
                }
                if(numWarehouses > loadWarehouses) {
                    errorMessage("numWarehouses cannot be greater than the warehouses loaded in the database");
                    throw new Exception();
                }

                try {
                    numTerminals = Integer.parseInt(iTerminals);
                    if(numTerminals <= 0 || numTerminals > 10 * numWarehouses) {
                        throw new NumberFormatException();
                    }
                } catch(NumberFormatException e1) {
                    errorMessage("Invalid number of terminals!");
                    throw new Exception();
                }

                if(Long.parseLong(iRunMins) != 0 && Integer.parseInt(iRunTxnsPerTerminal) == 0) {
                    try {
                        executionTimeMillis = Long.parseLong(iRunMins) * 60000;
                        if(executionTimeMillis <= 0) {
                            throw new NumberFormatException();
                        }
                    } catch(NumberFormatException e1) {
                        errorMessage("Invalid number of minutes!");
                        throw new Exception();
                    }
                } else {
                    try {
                        transactionsPerTerminal = Integer.parseInt(iRunTxnsPerTerminal);
                        if(transactionsPerTerminal <= 0) {
                            throw new NumberFormatException();
                        }
                    } catch(NumberFormatException e1) {
                        errorMessage("Invalid number of transactions per terminal!");
                        throw new Exception();
                    }
                }

                terminalWarehouseFixed = Boolean.parseBoolean(iTermWhseFixed);

                try {
                    newOrderWeightValue = Integer.parseInt(iNewOrderWeight);
                    paymentWeightValue = Integer.parseInt(iPaymentWeight);
                    orderStatusWeightValue = Integer.parseInt(iOrderStatusWeight);
                    deliveryWeightValue = Integer.parseInt(iDeliveryWeight);
                    stockLevelWeightValue = Integer.parseInt(iStockLevelWeight);

                    if(newOrderWeightValue < 0
                        || paymentWeightValue < 0
                        || orderStatusWeightValue < 0
                        || deliveryWeightValue < 0
                        || stockLevelWeightValue < 0) {
                        throw new NumberFormatException();
                    } else if(newOrderWeightValue == 0
                        && paymentWeightValue == 0
                        && orderStatusWeightValue == 0
                        && deliveryWeightValue == 0
                        && stockLevelWeightValue == 0) {
                        throw new NumberFormatException();
                    }
                } catch(NumberFormatException e1) {
                    errorMessage("Invalid number in mix percentage!");
                    throw new Exception();
                }

                if(newOrderWeightValue + paymentWeightValue + orderStatusWeightValue + deliveryWeightValue + stockLevelWeightValue > 100) {
                    errorMessage("Sum of mix percentage parameters exceeds 100%!");
                    throw new Exception();
                }

                newOrderCounter = 0;
                printMessage("Session started!");
                if(!limitIsTime) {
                    printMessage("Creating "
                        + numTerminals
                        + " terminal(s) with "
                        + transactionsPerTerminal
                        + " transaction(s) per terminal...");
                } else {
                    printMessage("Creating "
                        + numTerminals
                        + " terminal(s) with "
                        + (executionTimeMillis / 60000)
                        + " minute(s) of execution...");
                }
                if(terminalWarehouseFixed) {
                    printMessage("Terminal Warehouse is fixed");
                } else {
                    printMessage("Terminal Warehouse is NOT fixed");
                }
                printMessage("Transaction Weights: "
                    + newOrderWeightValue
                    + "% New-Order, "
                    + paymentWeightValue
                    + "% Payment, "
                    + orderStatusWeightValue
                    + "% Order-Status, "
                    + deliveryWeightValue
                    + "% Delivery, "
                    + stockLevelWeightValue
                    + "% Stock-Level");

                printMessage("Number of Terminals\t" + numTerminals);
            } catch(Exception ex) {
            }

            terminals = new TpccTerminal[numTerminals];
            terminalNames = new String[numTerminals];
            terminalsStarted = numTerminals;
            try {
                String database = iConn;
                String username = iUser;
                String password = iPassword;

                int[][] usedTerminals = new int[numWarehouses][10];
                for(int i = 0; i < numWarehouses; i++) {
                    for(int j = 0; j < 10; j++) {
                        usedTerminals[i][j] = 0;
                    }
                }

                for(int i = 0; i < numTerminals; i++) {
                    int terminalWarehouseID;
                    int terminalDistrictID;
                    do {
                        terminalWarehouseID = rnd.nextInt(1, numWarehouses);
                        terminalDistrictID = rnd.nextInt(1, 10);
                    }
                    while(usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] == 1);
                    usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] = 1;

                    String terminalName = "Term-" + (i >= 9 ? "" + (i + 1) : "0" + (i + 1));
                    Connection conn = null;
                    printMessage("Creating database connection for " + terminalName + "...");
                    conn = DriverManager.getConnection(database, dbProps);
                    conn.setAutoCommit(false);

                    TpccTerminal terminal = new TpccTerminal
                        (terminalName, terminalWarehouseID, terminalDistrictID,
                            conn, dbType,
                            transactionsPerTerminal, terminalWarehouseFixed,
                            paymentWeightValue, orderStatusWeightValue,
                            deliveryWeightValue, stockLevelWeightValue, numWarehouses, limPerMin_Terminal, this);

                    terminals[i] = terminal;
                    terminalNames[i] = terminalName;
                    printMessage(terminalName + "\t" + terminalWarehouseID);
                }

                sessionEndTargetTime = executionTimeMillis;
                signalTerminalsRequestEndSent = false;

                printMessage("Transaction\tWeight");
                printMessage("% New-Order\t" + newOrderWeightValue);
                printMessage("% Payment\t" + paymentWeightValue);
                printMessage("% Order-Status\t" + orderStatusWeightValue);
                printMessage("% Delivery\t" + deliveryWeightValue);
                printMessage("% Stock-Level\t" + stockLevelWeightValue);

                printMessage("Transaction Number\tTerminal\tType\tExecution Time (ms)\t\tComment");

                printMessage("Created " + numTerminals + " terminal(s) successfully!");
                boolean dummvar = true;

                // Create Terminals, Start Transactions
                sessionStart = getCurrentTime();
                sessionStartTimestamp = System.currentTimeMillis();
                sessionNextTimestamp = sessionStartTimestamp;
                if(sessionEndTargetTime != -1) {
                    sessionEndTargetTime += sessionStartTimestamp;
                }

                // Record run parameters in runInfo.csv
                if(runInfoCSV != null) {
                    try {
                        StringBuffer infoSB = new StringBuffer();
                        Formatter infoFmt = new Formatter(infoSB);
                        infoFmt.format("%d,simple,%s,%s,%s,%s,%d,%d,%d,%d,1.0,1.0\n",
                            runID, TPCC_VERSION, iDB,
                            new Timestamp(sessionStartTimestamp),
                            iRunMins,
                            loadWarehouses,
                            numWarehouses,
                            numTerminals,
                            Integer.parseInt(limPerMin));
                        runInfoCSV.write(infoSB.toString());
                        runInfoCSV.close();
                    } catch(Exception e) {
                        logger.error(e.getMessage());
                        System.exit(1);
                    }
                }

                synchronized(terminals) {
                    printMessage("Starting all terminals...");
                    transactionCount = 1;
                    for(int i = 0; i < terminals.length; i++) {
                        (new Thread(terminals[i])).start();
                    }

                }

                printMessage("All terminals started executing " + sessionStart);
            } catch(Exception e1) {
                errorMessage("This session ended with errors!");
                try {
                    printStreamReport.close();
                    fileOutputStream.close();
                } catch(IOException ignored) {}
            }
        }
        updateStatusLine();
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        new Tpcc();
    }

    private String getProp(Properties p, String pName) {
        String prop = p.getProperty(pName);
        logger.info("Term-00, " + pName + "=" + prop);
        return (prop);
    }

    private void signalTerminalsRequestEnd(boolean timeTriggered) {
        synchronized(terminals) {
            if(!signalTerminalsRequestEndSent) {
                if(timeTriggered) {
                    printMessage("The time limit has been reached.");
                }
                printMessage("Signalling all terminals to stop...");
                signalTerminalsRequestEndSent = true;

                for(int i = 0; i < terminals.length; i++) {
                    if(terminals[i] != null) {
                        terminals[i].stopRunningWhenPossible();
                    }
                }

                printMessage("Waiting for all active transactions to end...");
            }
        }
    }

    public void signalTerminalEnded(TpccTerminal terminal, long countNewOrdersExecuted) {
        synchronized(terminals) {
            boolean found = false;
            terminalsStarted--;
            for(int i = 0; i < terminals.length && !found; i++) {
                if(terminals[i] == terminal) {
                    terminals[i] = null;
                    terminalNames[i] = "(" + terminalNames[i] + ")";
                    newOrderCounter += countNewOrdersExecuted;
                    found = true;
                }
            }
        }

        if(terminalsStarted == 0) {
            sessionEnd = getCurrentTime();
            sessionEndTimestamp = System.currentTimeMillis();
            sessionEndTargetTime = -1;
            printMessage("All terminals finished executing " + sessionEnd);
            endReport();
            terminalsBlockingExit = false;
            printMessage("Session finished!");

            // If we opened a per transaction result file, close it.
            if(resultCSV != null) {
                try {
                    resultCSV.close();
                } catch(IOException e) {
                    logger.error(e.getMessage());
                }
            }

            // Stop the OsCollector, if it is active.
            if(osCollector != null) {
                osCollector.stop();
                osCollector = null;
            }
        }
    }

    public void signalTerminalEndedTransaction(String terminalName, String transactionType, long executionTime,
        String comment, int newOrder) {
        synchronized(counterLock) {
            transactionCount++;
            fastNewOrderCounter += newOrder;
        }

        if(sessionEndTargetTime != -1 && System.currentTimeMillis() > sessionEndTargetTime) {
            signalTerminalsRequestEnd(true);
        }

        updateStatusLine();

    }

    public TpccRandom getRnd() {
        return rnd;
    }

    public void resultAppend(TpccData term) {
        if(resultCSV != null) {
            try {
                resultCSV.write(runID + "," +
                    term.resultLine(sessionStartTimestamp));
            } catch(IOException e) {
                logger.error("Term-00, " + e.getMessage());
            }
        }
    }

    private void endReport() {
        long currTimeMillis = System.currentTimeMillis();
        long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
        long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
        double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
        double tpmTotal = (6000000 * transactionCount / (currTimeMillis - sessionStartTimestamp)) / 100.0;

        System.out.println();
        logger.info("Term-00, ");
        logger.info("Term-00, ");
        logger.info("Term-00, Measured tpmC (NewOrders) = " + tpmC);
        logger.info("Term-00, Measured tpmTOTAL = " + tpmTotal);
        logger.info("Term-00, Session Start     = " + sessionStart);
        logger.info("Term-00, Session End       = " + sessionEnd);
        logger.info("Term-00, Transaction Count = " + (transactionCount - 1));

    }

    private void printMessage(String message) {
        logger.trace("Term-00, " + message);
    }

    private void errorMessage(String message) {
        logger.error("Term-00, " + message);
    }

    private void exit() {
        System.exit(0);
    }

    private String getCurrentTime() {
        return dateFormat.format(new java.util.Date());
    }

    private String getFileNameSuffix() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return dateFormat.format(new java.util.Date());
    }

    synchronized private void updateStatusLine() {
        long currTimeMillis = System.currentTimeMillis();

        if(currTimeMillis > sessionNextTimestamp) {
            StringBuilder informativeText = new StringBuilder();
            Formatter fmt = new Formatter(informativeText);
            double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
            double tpmTotal = (6000000 * transactionCount / (currTimeMillis - sessionStartTimestamp)) / 100.0;

            sessionNextTimestamp += 1000;  /* update this every seconds */

            fmt.format("Term-00, Running Average tpmC: %.2f", tpmC);
            fmt.format("    Running Average tpmTOTAL: %.2f", tpmTotal);

            /* XXX What is the meaning of these numbers? */
            recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 12;
            recentTpmTotal = (transactionCount - sessionNextKounter) * 12;
            sessionNextKounter = fastNewOrderCounter;
            fmt.format("    Current tpmTOTAL: %d", recentTpmTotal);

            long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
            long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
            fmt.format("    Memory Usage: %dMB / %dMB          ", (totalMem - freeMem), totalMem);

            System.out.print(informativeText);
            for(int count = 0; count < 1 + informativeText.length(); count++) {
                System.out.print("\b");
            }
        }
    }
}

