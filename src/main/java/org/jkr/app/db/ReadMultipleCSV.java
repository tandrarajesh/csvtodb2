package org.jkr.app.db;

import java.io.File;

public class ReadMultipleCSV {

	public static void main(String[] args) {

		final File folder = new File("H:\\JPMC\\newcsvdemo\\CSVDemo2\\src\\main\\resources\\files");
		String arr = listFilesForFolder(folder);
	}

	public static String listFilesForFolder(final File folder) {
		String[] strarr = null;
		String filenames = null;
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				System.out.println(fileEntry.getName());
				filenames = fileEntry.getName();
				for (String string : strarr) {
					
				}
				
			}
		}
		return filenames;
	}

}
