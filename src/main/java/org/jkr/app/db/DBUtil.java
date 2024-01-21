package org.jkr.app.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

public class DBUtil {
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		Connection connection = null;
		//BufferedReader lineReader = null;
		PreparedStatement pstmt = null;
		Properties prop = readPropertiesFile("src/main/resources/table.properties");
		String csvFilePath = prop.getProperty("csvFilePath");
		String Drivername = prop.getProperty("DriverClassName");
		String url = prop.getProperty("url");
		String username = prop.getProperty("username");
		String password = prop.getProperty("password");
		String tableName = prop.getProperty("table_name");
		int batchSize = 50;
		String lineText = null;
		final File folder = new File("H:\\JPMC\\newcsvdemo\\CSVDemo2\\src\\main\\resources\\files");
		try(BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath)); ) {
			System.out.println("Connecting to database...");
			//String csvFilePath = "src/main/resources/file.csv";
			// listFilesForFolder(folder);
			Class.forName(Drivername);
			connection = DriverManager.getConnection(url, username, password);
			connection.setAutoCommit(false);
			//lineReader = new BufferedReader(new FileReader(csvFilePath));
			String[] headerRow = lineReader.readLine().split(",");
			List<String> headerRows = Arrays.asList(headerRow);
//            lineReader.readLine(); // skip header line
			String questionmarks = StringUtils.repeat("?,", headerRow.length);
			questionmarks = (String) questionmarks.subSequence(0, questionmarks.length() - 1);
			String sql = "INSERT INTO " + tableName + " (" + String.join(",", headerRow) + ") VALUES (" + questionmarks	+ ")";
			System.out.println("Insert statement : " + sql);
			pstmt = connection.prepareStatement(sql);
			int count = 0;
			while ((lineText = lineReader.readLine()) != null) {
				String[] data = lineText.split(",");
				for (int i = 0; i < data.length; i++) {
					pstmt.setString(i+1, data[i]);
				}
				pstmt.addBatch();
				
				  if (count % batchSize == 0) {
					  pstmt.executeBatch();
					  }
			}
			lineReader.close();
			// execute the remaining queries
			pstmt.executeBatch();  //need to remove comment...........................
			connection.commit();
			connection.close();
			System.out.println("successfully inserted");
		} catch (IOException ex) {
			System.err.println(ex);
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static Properties readPropertiesFile(String fileName) throws IOException {
		FileInputStream fis = null;
		Properties prop = null;
		try {
			fis = new FileInputStream(fileName);
			prop = new Properties();
			prop.load(fis);
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			fis.close();
		}
		return prop;
	}
	
	public static String[] listFilesForFolder(final File folder) {
		String[] filenames = null;
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				System.out.println(fileEntry.getName());
				return filenames;
			}
		}
		return filenames;
	}
}