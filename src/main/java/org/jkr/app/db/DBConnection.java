package org.jkr.app.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {

	private String url;
	private String username;
	private String password;
	
	private Connection connection = null;

	public DBConnection(String url, String username, String password) {
		this.url = url;
		this.username = username;
		this.password = password;
	}
	
	public Connection getConnection() {
		try {
			connection = DriverManager.getConnection(url,username,password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}
	
}
