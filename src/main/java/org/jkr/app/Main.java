package org.jkr.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class Main {
    public static void main(String[] args) throws ClassNotFoundException {
        Connection connection = null;
        PreparedStatement preparedstatement = null;
        BufferedReader lineReader = null;
        try {
            System.out.println("Connecting to database...");
            String csvFilePath = "src/main/resources/review.csv";

            int batchSize = 20;

            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/Demo", "root", "Password");

            connection.setAutoCommit(false);

            String sql = "INSERT INTO reviews (course_name, student_name) VALUES (?, ?)";
            System.out.println("Insert statement : " + sql);
            preparedstatement = connection.prepareStatement(sql);
            lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;
            int count = 0;
            lineReader.readLine(); // skip header line
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
                String courseName = data[0];
                String studentName = data[1];
                preparedstatement.setString(1, courseName);
                preparedstatement.setString(2, studentName);
                preparedstatement.addBatch();

                if (count % batchSize == 0) {
                	preparedstatement.executeBatch();
                }
            }

            lineReader.close();

            // execute the remaining queries
            preparedstatement.executeBatch();

            connection.commit();
            connection.close();

        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

        }

    }
}

