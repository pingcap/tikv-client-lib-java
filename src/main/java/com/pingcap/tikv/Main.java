package com.pingcap.tikv;


import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Main {
  public static void main(String args[]) throws Exception {
    String myDriver = "com.mysql.jdbc.Driver";
    String myUrl = "jdbc:mysql://127.0.0.1:4000/tpch50";
    Class.forName(myDriver);
    Connection conn = DriverManager.getConnection(myUrl, "root", "");
    conn.setAutoCommit(true);
    String query = " insert into lineitem"
        + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement preparedStmt = conn.prepareStatement(query);
    int inc = 21464623;
    Date date = Date.valueOf("2000-01-01");
    preparedStmt.setInt (1, inc);
    preparedStmt.setInt (2, inc);
    preparedStmt.setInt (3, inc);
    preparedStmt.setInt (4, inc);
    preparedStmt.setDouble (5, 1.1);
    preparedStmt.setDouble (6, 1.1);
    preparedStmt.setDouble (7, 1.1);
    preparedStmt.setDouble (8, 1.1);
    preparedStmt.setString (9, "a");
    preparedStmt.setString (10, "c");
    preparedStmt.setDate   (11, date);
    preparedStmt.setDate   (12, date);
    preparedStmt.setDate   (13, date);
    preparedStmt.setString (14, "test");
    preparedStmt.setString (15, "test");
    preparedStmt.setString (16, "test");
    for (int i = 0; i < 100000000; i++) {
      for (int j = 0; j < 1000; j++) {
        inc++;
        preparedStmt.setInt(1, inc);
        preparedStmt.addBatch();
      }
      preparedStmt.executeBatch();
    }
    conn.close();
  }
}
