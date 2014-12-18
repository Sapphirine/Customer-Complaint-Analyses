/*
 http://github.com/avinsrid

 Following Code converts a CSV file with N columns to M columns where M < N. It will also sort the CSV file based on a specific column 
 Here we will split into the columns [Product, Complaint ID, Issue, State]
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

public class ColumnReducerSorter {

	public static void main (String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("ColumnReducerSorter::main(): Arguments: [Input CSV File] [Output Sequence Directory]");
		}

		Map<String, Integer> fieldIndex = new HashMap<String, Integer>();
		String inputFile = args[0];
		String outputDir = args[1];
		String inputFields;
		Scanner input = new Scanner(System.in);
		String[] fields = {"Product", "Complaint ID", "Issue", "State"};

		int count = 0;

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		String everyLine = reader.readLine();
		if (everyLine!= null) {

			// We have 14 columns to split into. We also determine the index of the above four columns, as they can be any columns in our tabulated
			// CSV file
			String[] fieldSplitter = everyLine.split(",", 14);
			Set<String> fieldSet = new HashSet<String>(Arrays.asList(fields));
			for (int i = 0; i < fieldSplitter.length; i = i + 1) {
				if (fieldSet.contains(fieldSplitter[i])) {
					fieldIndex.put(fieldSplitter[i], i);
				}
			}
		}
		FileWriter writer = new FileWriter("/home/blitzavi89/BigData_Project/PROJECT-FILES/data/SortedData.csv");
		System.out.println(fieldIndex);
		while (true) {
			everyLine = reader.readLine();
			if (everyLine == null) {
				break;
			}
			String[] columns = everyLine.split(",", 14);
			if (columns.length != 14) {
				System.out.println("ColumnReducerSorter::main(): Invalid Line");
			}
			String product = columns[fieldIndex.get("Product")];
			String complaintID = columns[fieldIndex.get("Complaint ID")];
			String issue = columns[fieldIndex.get("Issue")];
			String state = columns[fieldIndex.get("State")];
			try {
				writer.append(state);
				writer.append(',');
				writer.append(product);
				writer.append(',');
				writer.append(complaintID);
				writer.append(',');
				writer.append(issue);
				writer.append('\n');
			}
			catch (IOException e) {System.out.println("ColumnReducerSorter::main(): Error writing File"); }
			count ++;
		}
		reader.close();
		writer.close();
		System.out.println("ColumnReducerSorter::main() Reduced " + count + " files to required number of columns ");
	}
}
