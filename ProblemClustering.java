package complaintMetric;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import com.csvreader.CsvReader;

public class ProblemClustering {
	
	static String fileName = null;
	static String[] headers = {"Product", "Issue", "Submitted via", "Date received", "Company response", 
								"Timely response?", "Consumer disputed?" };
	
	static double exponent = 2.718282;
	
	static HashMap<String, Integer> productList;
	static HashMap<String, Integer> issueList;
	
	static HashMap<String, Integer> productCompanyResponseCount; /*Hashes a concat of 2 strings - product and response*/
	static HashMap<String, Integer> productTimelyResponseCount;
	static HashMap<String, Integer> productConsumerDisputedCount;

	static HashMap<String, Integer> issueCompanyResponseCount; /*Hashes a concat of 2 strings - issue and response*/
	static HashMap<String, Integer> issueTimelyResponseCount;
	static HashMap<String, Integer> issueConsumerDisputedCount; 
	
	
	private void initLists() {
		
		productList = new HashMap<String, Integer>();
		issueList = new HashMap<String, Integer>();
		productCompanyResponseCount = new HashMap<String, Integer> ();
		productTimelyResponseCount = new HashMap<String, Integer>();
		productConsumerDisputedCount = new HashMap<String, Integer>();
		
		issueCompanyResponseCount = new HashMap<String, Integer> ();
		issueTimelyResponseCount = new HashMap<String, Integer> ();
		issueConsumerDisputedCount = new HashMap<String, Integer> ();
		
	
	}
	

	static CsvReader readComplaintsCSVFile() throws IOException {
		
		CsvReader complaints = null;
		complaints = new CsvReader(fileName);
		complaints.readHeaders();
		return complaints;
	}
	 

	
	static LinkedHashSet<String> getCategories(CsvReader complaints, String category) throws IOException {
		
		if (complaints == null)
			return null;
		LinkedHashSet<String> issues = new LinkedHashSet<String>();
		
		while (complaints.readRecord()) {
			
			String issue = complaints.get(category);
			if(!issues.contains(issue)) {
				
				issues.add(issue);
			}
						
		}
		

		return issues;
		
	}
	
	
	private void setupHashTables(CsvReader csvReader) throws IOException {
		
		csvReader.readHeaders();
		
		while(csvReader.readRecord()) {
			
			String issue = csvReader.get("Product");
					
			if (productList.containsKey(issue)) {
				
				
				productList.put(issue, productList.get(issue) + 1);
			}
			else {
				
				productList.put(issue, 1);
				
			}
			
		}
		
		csvReader = readComplaintsCSVFile();
		csvReader.readHeaders();
		
		while(csvReader.readRecord()) {
			
			String issue = csvReader.get("Issue");
						
			if (issueList.containsKey(issue)) {
				
				issueList.put(issue, issueList.get(issue) + 1);
			}
			else {
				
				issueList.put(issue, 1);
				
			}
			
		}
		
	
	csvReader = readComplaintsCSVFile();
	csvReader.readHeaders();
	
	while (csvReader.readRecord()) {
		
		String toHash = null;
		String product = csvReader.get("Product");
		String companyResponse = csvReader.get("Company response");
		
		toHash = product + companyResponse;
			
		if(productCompanyResponseCount.containsKey(toHash)){
			
			productCompanyResponseCount.put(toHash, productCompanyResponseCount.get(toHash) + 1);
			
		}
		else {
			productCompanyResponseCount.put(toHash, 1);
		}
		
		
	}
	
	csvReader = readComplaintsCSVFile();
	csvReader.readHeaders();
	
	while (csvReader.readRecord()) {
		
		String product = csvReader.get("Product");
		String timelyResponse = csvReader.get("Timely response?");
		
		if (productTimelyResponseCount.containsKey(product)) {
			
			if (timelyResponse.equalsIgnoreCase("yes")) {
				
				productTimelyResponseCount.put(product, productTimelyResponseCount.get(product) + 1);
							
			}
					
		}
		else {
			
			productTimelyResponseCount.put(product, 1);
			
		}
		
		
		
	}
	
	csvReader = readComplaintsCSVFile();
	csvReader.readHeaders();
	
	while (csvReader.readRecord()) {
		
		String product = csvReader.get("Product");
		String timelyResponse = csvReader.get("Consumer disputed?");
		
		if (productConsumerDisputedCount.containsKey(product)) {
			
			if (timelyResponse.equalsIgnoreCase("yes")) {
				
				productConsumerDisputedCount.put(product, productConsumerDisputedCount.get(product) + 1);
							
			}
					
		}
		else {
			
			productConsumerDisputedCount.put(product, 1);
			
		}
		
	}
	
	csvReader = readComplaintsCSVFile();
	csvReader.readHeaders();
	
	
	while (csvReader.readRecord()) {
		
		String toHash = null;
		String product = csvReader.get("Issue");
		String companyResponse = csvReader.get("Company response");
		
		toHash = product + companyResponse;
			
		if(issueCompanyResponseCount.containsKey(toHash)){
			
			issueCompanyResponseCount.put(toHash, issueCompanyResponseCount.get(toHash) + 1);
			
		}
		else {
			issueCompanyResponseCount.put(toHash, 1);
		}
		
		
	}
	
	csvReader = readComplaintsCSVFile();
	csvReader.readHeaders();
	
	while (csvReader.readRecord()) {
		
		String product = csvReader.get("Issue");
		String timelyResponse = csvReader.get("Timely response?");
		
		if (issueTimelyResponseCount.containsKey(product)) {
			
			if (timelyResponse.equalsIgnoreCase("yes")) {
				
				issueTimelyResponseCount.put(product, issueTimelyResponseCount.get(product) + 1);
							
			}
					
		}
		else {
			
			issueTimelyResponseCount.put(product, 1);
			
		}
		
		
		
	}
	
	csvReader = readComplaintsCSVFile();
	csvReader.readHeaders();
	
	while (csvReader.readRecord()) {
		
		String product = csvReader.get("Issue");
		String timelyResponse = csvReader.get("Consumer disputed?");
		
		if (issueConsumerDisputedCount.containsKey(product)) {
			
			if (timelyResponse.equalsIgnoreCase("yes")) {
				
				issueConsumerDisputedCount.put(product, issueConsumerDisputedCount.get(product) + 1);
							
			}
					
		}
		else {
			
			issueConsumerDisputedCount.put(product, 1);
			
		}
		
	}
	
	
	}
	
	public ProblemClustering (String fileName) throws IOException {
		
		this.fileName = fileName;
		initLists();
		
		CsvReader csvReader = null;
		
		csvReader = readComplaintsCSVFile();
		
		setupHashTables(csvReader);
		
		
		
	}
	
	
	public static void calculateProductAverageMetric() throws IOException, InterruptedException {
		
		CsvReader localCsvReader = null;
		localCsvReader = readComplaintsCSVFile();
		
		localCsvReader.readHeaders();
		
		HashMap<String, Double> productMetricStore = new HashMap<String, Double>();
		
		while (localCsvReader.readRecord()) {
			
			int complaintID = Integer.parseInt(localCsvReader.get("Complaint ID"));
			String product = localCsvReader.get("Product");
			String issue = localCsvReader.get("Issue");
			String submissionMethod = localCsvReader.get("Submitted via");
			String companyResponse = localCsvReader.get("Company response");
			
			
			
			//System.out.println(complaintID + " >> " + product + " >> " + issue + " >> " + companyResponse);
			
			
			double probProduct = (double)productList.get(product)/48085;
			double probIssue = (double)issueList.get(issue)/48005;
			
			double probProductCompanyResponse = (double)productCompanyResponseCount.get(product+companyResponse)/(double)productList.get(product);
			double probIssueCompanyResponse = (double)issueCompanyResponseCount.get(issue+companyResponse)/(double)issueList.get(issue);
			
			
			double metric = 0;
			
			double productContrib = Math.pow(exponent, -probProduct);
			double issueContrib =  Math.pow(exponent, -probIssue);
			double productCompanyResponseContrib = Math.pow(exponent, probProductCompanyResponse);
			double issueCompanyResponseContrib = Math.pow(exponent, probIssueCompanyResponse);
			
			double submissionMethodContrib = 0;
			
			if (submissionMethod.equalsIgnoreCase("phone")){
				
				submissionMethodContrib = 1;
							
			}
			
			else if (submissionMethod.equalsIgnoreCase("email") || submissionMethod.equalsIgnoreCase("web")) {
				
				submissionMethodContrib = 2;
				
				
			}
			
			else {
				
				submissionMethodContrib = 3;
			}
			
			submissionMethodContrib = Math.sqrt(6.2832)/submissionMethodContrib;
			
			metric = productContrib + issueContrib + productCompanyResponseContrib + issueCompanyResponseContrib + submissionMethodContrib;
			
			if(productMetricStore.containsKey(product)) {
				
				productMetricStore.put(product, productMetricStore.get(product) + metric);
							
			}
			else {
				
				productMetricStore.put(product, metric);
				
			}
			
			
			//System.out.println(complaintID + " >> " + probProduct + " >> " + probIssue + " >> " + probProductCompanyResponse + " >> " + probIssueCompanyResponse);
		
		}
		
		System.out.println("Average scores based on products");
		
		for (String product_temp: productMetricStore.keySet()) {
			
			double product_metric = productMetricStore.get(product_temp);
			double average = product_metric / (double)productList.get(product_temp);
			
			System.out.println(product_temp + " >> " + average);
			
		}
		
		
		
		
	}
	
	
	public static void main(String[] args) throws IOException, InterruptedException {
		
		ProblemClustering problemClustering = new ProblemClustering ("complaints_filtered.csv");
		

		calculateProductAverageMetric();
			
			

		
				

	}

}
