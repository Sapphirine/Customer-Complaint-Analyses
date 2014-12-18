package com.bigdata.complaintanalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
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
import java.lang.Runtime;
import java.lang.Process;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class  ClassificationAutomator {

	public static void main(String[] args) throws Exception {

		// args[] needs to be the input raw data which will go through cleaning phase followed by Mahout classification
		if (args.length != 1) {
			System.err.println("ClassificationAutomator::main(): Not supplied raw data file to process");
		}

		String inputFile = args[0];
		String outputDir = "data/";

		// This will be relative to HDFS system path
		String mahoutSeqOutput = "data/";

		// We first take the data and split it into required number of columns, here we choose 4 --> [Product, Complaint ID, Issue, State]. Second input
		// to ColumnReducer's method will need to be hardcoded, i.e. reduceColumn(inputFile, "data")
		ColumnReducer reducer = new ColumnReducer();
		reducer.reduceColumn(inputFile, outputDir);

		// Now call Nachiket's class to obtain the state wise sorted and bank wise sorted data. Note that state sorted data goes into data/states
		// and banks sorted data goes into data/banks
		StatewiseSorter stateSorter = new StatewiseSorter();
		File stateDir = new File("data/states");
		if (!stateDir.exists()) {
			stateDir.mkdir();
		}
		File classificationDir = new File("data/classification");
		if (!stateDir.exists()) {
			classificationDir.mkdir();
		}
		stateSorter.sortState(outputDir, outputDir + "states/");

		ColumnReducerBank bankReducer = new ColumnReducerBank();
		bankReducer.reduceColumn(inputFile, outputDir);
		File bankDir = new File("data/banks");
		if (!bankDir.exists()) {
			bankDir.mkdir();
		}
		BankwiseSorter bankSorter = new BankwiseSorter();
		bankSorter.sortBank(outputDir, outputDir + "banks/");

		// We convert every CSV file into a sequence file by calling ComplaintsCSVtoSeq. We will also iterate over every file under data/states and call
		// the method convertToSeq(). Before that we need to strip the first column i.e. state of each csv file.
		ComplaintsCSVtoSeq allComplaints = new ComplaintsCSVtoSeq();
		StripColumn allColumns = new StripColumn();
		File[] directoryList = stateDir.listFiles();

		for (File everyFile : directoryList) {
			if (everyFile.exists()) {
			
				// Note that the file name is the same as temp_stateName
				String fileName = everyFile.getName();
				System.out.println(fileName);
				String[] tempFileName = fileName.split("_",2);
				String[] targetFileName = tempFileName[1].split("\\.", 2);
				File dirStateCreate = new File("data/classification/" + targetFileName[0]);
				System.out.println("ClassificationAutomator::main(): Created directory " + "data/classification/" + targetFileName[0]);
				if (!dirStateCreate.exists()) {
					dirStateCreate.mkdir();
				}
				allColumns.deleteColumn(fileName);
			}
		}
		// We also need to push these files into hadoop as the convertToSeq() call will execute on HDFS
		Process p = Runtime.getRuntime().exec("hdfs dfs -mkdir /data");
		p = Runtime.getRuntime().exec("hdfs dfs -put data/states data/");
		p = Runtime.getRuntime().exec("hdfs dfs -put data/classification data/");
		p = Runtime.getRuntime().exec("hdfs dfs -put data/banks data/");
		File[] newDirectoryList = stateDir.listFiles();
		for (File everyFile : newDirectoryList) {
			if (everyFile.exists()) {
			
				// Note that the file name is the same as temp_stateName
				String fileName = everyFile.getName();
				String[] targetFileName = fileName.split("\\.",2);
				allComplaints.convertToSeq("data/states/" + fileName, mahoutSeqOutput + "classification/" + targetFileName[0]);
			}
		allComplaints.convertToSeq("data/banks/Equifax.csv", mahoutSeqOutput + "classification/");
		}
	} 
}