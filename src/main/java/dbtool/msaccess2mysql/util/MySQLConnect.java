package dbtool.msaccess2mysql.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Statement;

public class MySQLConnect {
	private Connection conn = null;
	private String serverName;
	private int port;
	private String username;
	private String password;
	private String schemeName;
	private String url;

	public MySQLConnect(String serverName, int port, String username, String password, String schemeName) {
		super();
		this.serverName = serverName;
		this.port = port;
		this.username = username;
		this.password = password;
		this.schemeName = schemeName;
		this.url = "jdbc:mysql://" + serverName + ":" + port + "/" + schemeName + "?autoReconnect=true&useSSL=false";
	}

	public void connect(boolean autoCommit) throws Exception {
		Class.forName("org.gjt.mm.mysql.Driver").newInstance();
//		conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/generali?autoReconnect=true&useSSL=false", "root", "root");
		conn = DriverManager.getConnection(this.url, this.username, this.password);
		conn.setAutoCommit(autoCommit);
	}

	public void select() throws SQLException {
		Statement stmt = (Statement) conn.createStatement();
		ResultSet rs = stmt.executeQuery("select * from product");
		while (rs.next()) {
			System.out.println(rs.getInt(1) + "  " + rs.getString(2) + "  " + rs.getString(3));
		}
	}

	public boolean isTableExist(String tableName) throws SQLException {
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet res = meta.getTables(null, null, tableName, new String[] { "TABLE" });
		if (res.next()) {
			return true;
		}
		return false;
	}

	public ResultSet getColumns(String tableName) throws SQLException {
	    String sql = "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA='" + this.schemeName + "' AND TABLE_NAME='"+ tableName + "'";
	    java.sql.Statement stmt = conn.createStatement();
	    ResultSet rs = stmt.executeQuery(sql);
	    return rs;
	}
	
	public void createTable(String tableName) throws SQLException {
		String sqlCreate = "CREATE TABLE IF NOT EXISTS " + tableName
				+ "  (brand           VARCHAR(10),"
				+ "   year            INTEGER,"
				+ "   number          INTEGER,"
				+ "   value           INTEGER,"
				+ "   card_count           INTEGER,"
				+ "   player_name     VARCHAR(50),"
				+ "   player_position VARCHAR(20))";
		
		java.sql.Statement stmt = conn.createStatement();
		stmt.execute(sqlCreate);
	}
	
	public void addColumn(String tableName, String colnameTypePair) throws SQLException {
		String sqlCreate = "ALTER TABLE `" + this.schemeName + "`.`" + tableName + "` ADD COLUMN " + colnameTypePair + " NULL";
		java.sql.Statement stmt = conn.createStatement();
		stmt.execute(sqlCreate);
	}
	
	public void createTable(String tableName, String colnameTypePairs) throws SQLException {
	    String sqlCreate = "CREATE TABLE IF NOT EXISTS " + tableName
	            + " (" 
	            + " `row_id` int unsigned NOT NULL AUTO_INCREMENT,"
	            + colnameTypePairs
	            + ",`created_date` DATETIME NOT NULL DEFAULT now()"
	            + ",`updated_date` DATETIME NOT NULL DEFAULT now()"
	            + ", PRIMARY KEY (`row_id`)"
	    		+ ") DEFAULT CHARSET=utf8;";
	    java.sql.Statement stmt = conn.createStatement();
	    System.out.println(sqlCreate);
	    stmt.execute(sqlCreate);
	}
	
	/**
	 * TRUNCATE TABLE in a database transaction will cause a implicit COMMIT action. 
	 * DELETE does not have that same behavior, so you can safely run
	 * @param tableName
	 * @throws SQLException
	 */
	public void truncate(String tableName) throws SQLException {
		  java.sql.Statement stmt = conn.createStatement();
		  stmt.executeUpdate("TRUNCATE TABLE "+ tableName);
	}
	
	public void delete(String tableName) throws SQLException {
		  java.sql.Statement stmt = conn.createStatement();
		  stmt.executeUpdate("DELETE FROM "+ tableName);
	}
	
	/**
	 * 
	 * @param tableName
	 * @param columns: colname1, colname2, colname3
	 * @param values: (val1,val2,val3),(val4,val5,val6),(val7,val8,val9)
	 * @throws SQLException
	 */
	public void insert(String tableName, String columns, String values) throws SQLException {
		String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES " + values + ";";
//		System.out.println(sql);
		 java.sql.Statement stmt = conn.createStatement();
		 stmt.executeUpdate(sql);
	}
	
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		conn.setAutoCommit(autoCommit);
	}
	
	public void commit() throws SQLException {
		conn.commit();
	}
	
	public void rollback() throws SQLException {
		conn.rollback();
	}
	
	public void close() throws SQLException {
		conn.close();
	}
}
