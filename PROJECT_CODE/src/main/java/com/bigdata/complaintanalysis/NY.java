/*
 http://github.com/avinsrid

Following code will take one specific state from the SortedData obtained from ColumnReducerSorter.java and create a CSV file with columns
[Product, Complaint ID, Issue] and output it to data/$state_name.csv
 */

package com.bigdata.complaintanalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
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

public class NY {

	public static void main (String[] args) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader("data/SortedData.csv"));
		FileWriter writer = new FileWriter("data/NY.csv");
		int count = 0;
		while(true) {
			String everyLine = reader.readLine();
			if (everyLine == null) {
				break;
			}

			// SortedData.csv has 4 columns, hence we split it into 4 as shown below and each column is separated by ,
			String[] columns = everyLine.split(",", 4);

			if (columns[3].equals("NY")) {
				try {
					writer.append(columns[0]);
					writer.append(',');
					writer.append(columns[1]);
					writer.append(',');
					writer.append(columns[2]);
					writer.append('\n');
				}
				catch(IOException e) {System.err.println("NY::main(): Exception caught during file write"); }
				count++;
			}
		}
		reader.close();
		writer.close();
		System.out.println("NY::main(): Successfully written " + count + " rows with NY state");
	}
}