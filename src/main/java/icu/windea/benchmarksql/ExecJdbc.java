/*
 * Copyright (C) 2004-2016, Denis Lussier
 * Copyright (C) 2016, Jan Wieck
 *
 * Copyright (C) 2021, DragonKnightOfBreeze
 */

package icu.windea.benchmarksql;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Command line program to process SQL DDL statements, from a text input file, to any JDBC Data Source.
 */
public class ExecJdbc {
    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt;
        String rLine;
        StringBuilder sql = new StringBuilder();
        try {
            Properties ini = new Properties();
            ini.load(new FileInputStream(System.getProperty("prop")));

            // Register jdbcDriver
            Class.forName(ini.getProperty("driver"));

            // make connection
            conn = DriverManager.getConnection(ini.getProperty("conn"), ini.getProperty("user"), ini.getProperty("password"));
            conn.setAutoCommit(true);

            // Create Statement
            stmt = conn.createStatement();

            // Open inputFile
            BufferedReader in = new BufferedReader(new FileReader(TpccUtil.getSysProp("commandFile", null)));

            // loop thru input file and concatenate SQL statement fragments
            while((rLine = in.readLine()) != null) {
                String line = rLine.trim();
                
                if(line.length() != 0) {
                    if(line.startsWith("--")) {
                        System.out.println(line);  // print comment line
                    } else {
                        if(line.endsWith("\\;")) {
                            sql.append(line.replaceAll("\\\\;", ";"));
                            sql.append("\n");
                        } else {
                            sql.append(line.replaceAll("\\\\;", ";"));
                            if(line.endsWith(";")) {
                                String query = sql.toString();

                                execJDBC(stmt, query.substring(0, query.length() - 1));
                                sql = new StringBuilder();
                            } else {
                                sql.append("\n");
                            }
                        }
                    }
                }
            }

            in.close();
        } catch(IOException | SQLException ie) {
            System.out.println(ie.getMessage());
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(conn != null) conn.close();
            } catch(SQLException se) {
                se.printStackTrace();
            }
        }
    }
    
    static void execJDBC(Statement stmt, String query) {
        System.out.println(query + ";");
        try {
            stmt.execute(query);
        } catch(SQLException se) {
            System.out.println(se.getMessage());
        }
    }
}
