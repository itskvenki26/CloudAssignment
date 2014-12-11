

//package twitLocationTrends;

import java.sql.*;

public class SQLiteJDBC {
	public static void main(String args[]) {
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:newtweet.db");
			System.out.println("Opened database successfully");

			stmt = c.createStatement();
			// String sql = "CREATE TABLE twitLocation4 " +
			// "( twit_ID TEXT PRIMARY KEY NOT NULL, " +
			// "User_Name TEXT NOT NULL, " +
			// // " message TEXT," +
			// "Date_time TEXT )";
			String sql = "CREATE TABLE twitLocation "
					+ "( twit_ID TEXT PRIMARY KEY NOT NULL, "
					+ "User_Name TEXT NOT NULL, "					
					+  "Date_time TEXT )";
			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Table created successfully");

	}
}
