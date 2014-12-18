/* This code takes the trimmed dataset with 4 columns and makes new csv files matching the first column - this is like code uploaded
by avinash except files made dynamically iterating through the entire dataset and adding. Runtime not too bad ~15/20 secs

Can be modified to split into different files based on different paramters by setting the parameter to 1st column in csv and playing
around with a few lines of code
*/

package com.bigdata.complaintanalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class BankwiseSorter {

	public void sortBank(String args1, String args2) throws FileNotFoundException {
		File file = new File(args1 + "BankSortedData.csv");
		FileReader fileReader = new FileReader(file);
		BufferedReader Readline = new BufferedReader(fileReader);
		String Line, parameter;
		FileWriter writeFile = null;
		ArrayList<String> states = new ArrayList<String>();
		Map<String,String> statefiles = new HashMap<String,String>();
		try {
			while((Line = Readline.readLine()) !=null) {
				int count=0;
				boolean Flag = false;
				Scanner scanLine = new Scanner(Line).useDelimiter(",");
				while(scanLine.hasNext()) {
					count++;
					parameter = scanLine.next();
					if(count==1 && parameter.length()!=0) {
						if(writeFile!=null) writeFile.close();
						if(!statefiles.containsKey(parameter)) {
							
							System.out.println("StatewiseSorter::sortState() No match exists for state :" + parameter + "\nMake new file");
							states.add(parameter);
							String path = args2 + "temp_" + parameter +".csv";
							statefiles.put(parameter, path);
							writeFile = new FileWriter(path);
							System.out.println("StatewiseSorter::sortState() No match exists for state :" + parameter);
						}
						else {
							
							writeFile = new FileWriter(statefiles.get(parameter),true);
						}
					}
					else if(count==1 && parameter.length()==0) { 
						Flag = true;
						break;
					}
					if(parameter !=null) writeFile.append(parameter);
					if(parameter !=null) writeFile.append(", ");
				}
				if(!Flag) writeFile.append("\n");
			}
		} catch (IOException e) 
		{
			System.out.println("StatewiseSorter::sortState() Error with file");
			e.printStackTrace();
		}
	}
}
