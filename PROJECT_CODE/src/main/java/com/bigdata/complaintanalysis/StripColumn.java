/*
 http://github.com/avinsrid

Following code will take one specific state from the SortedData obtained from ColumnReducerSorter.java and create a CSV file with columns
[Product, Complaint ID, Issue] and output it to data/state/$state_name.csv
 */

package com.bigdata.complaintanalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.File;
import java.util.Scanner;
import java.lang.String;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.io.IOException;

public class StripColumn {

	public void deleteColumn (String stateName) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader("data/states/" + stateName));
		File toBeDeletedFile = new File("data/states/" + stateName);

		// Extract the temp from temp_StateName, by splitting the string stateName
		String[] targetFileName = stateName.split("_", 2);
		FileWriter writer = new FileWriter("data/states/" + targetFileName[1]);
		int count = 0;
		while(true) {
			String everyLine = reader.readLine();
			if (everyLine == null) {
				break;
			}

			// SortedData.csv has 4 columns, hence we split it into 4 as shown below and each column is separated by ,
			String[] columns = everyLine.split(",", 4);

			if (columns[0].equals(targetFileName[1].substring(0,2))) {
				try {
					writer.append(columns[1]);
					writer.append(',');
					writer.append(columns[2]);
					writer.append(',');
					writer.append(columns[3]);
					writer.append('\n');
				}
				catch(IOException e) {System.err.println("StripColumn::deleteColumn(): Exception caught during file write"); }
				count++;
			}
		}
		reader.close();
		writer.close();

		// Delete the temp file, as it was raw and had 4 columns.
		toBeDeletedFile.delete();
		System.out.println("StripColumn::deleteColumn(): Successfully written " + count + " rows with " + targetFileName[1] + " state");
	}
}