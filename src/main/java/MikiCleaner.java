// Projection: Miki Mining
// Author: Danh Doan
// Class: MikiCleaner

import org.w3c.dom.*;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import javax.xml.stream.*;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.events.StartElement;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MikiCleaner {
	static int featureThreshold = 1;
	static List<String> featureList = new ArrayList();
	static File cleanedDatasetFile = new File("datasets/cleaned_wiki_articles_07.txt");
	static File featureFile = new File("datasets/feature_list_07.txt");
	static File dict = new File("database/google.txt");
	static File stopWordFile = new File("database/stopwords.txt");
	static List<String> dictList = new ArrayList(); 
	static List<String> stopwordList = new ArrayList();
	static long tupleSize = 0; 
	static long articleTotal = 0;
	public static void main(String[] args) throws IOException {
		// Load dictionary and stop word list
		loadDictionary();	
		System.out.println("Load dictionary. Size = " +  dictList.size());

		// Clean subset of dataset and integrate them into final dataset
		cleanData("datasets/wiki_articles_p1.xml", false);	
		cleanData("datasets/wiki_articles_p2.xml", true);
		cleanData("datasets/wiki_articles_p3.xml", true);
		cleanData("datasets/wiki_articles_p4.xml", true);
		cleanData("datasets/wiki_articles_p5.xml", true);
		cleanData("datasets/wiki_articles_p6.xml", true);
		cleanData("datasets/wiki_articles_p7.xml", true);
		cleanData("datasets/wiki_articles_p8.xml", true);
	}

	// Function: clean Data
	static void cleanData(String xmlFile, Boolean append) throws IOException {
		// Prepare output cleaned dataset file
		BufferedWriter bw = new BufferedWriter(new FileWriter(cleanedDatasetFile, append));
	
		// Prepare output feature list file
		BufferedWriter bw2 = new BufferedWriter(new FileWriter(featureFile, append));
		
		
		// Read the wiki dataset
		try {
			FileInputStream fileInputStream = new FileInputStream(xmlFile);
			XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
			XMLEventReader xmlEventReader = xmlFactory.createXMLEventReader(fileInputStream);
			while (xmlEventReader.hasNext()) {
					XMLEvent xmlEvent = xmlEventReader.nextEvent();
				if(xmlEvent.isStartElement()) {
					StartElement startElement = xmlEvent.asStartElement();
					if (startElement.getName().getLocalPart().equals("text")) {
						xmlEvent = xmlEventReader.nextEvent();
						if (xmlEvent.isCharacters()) {
						String pageText = xmlEvent.asCharacters().getData().toString();
						pageText = pageText.replaceAll("[^a-zA-Z0-9\\s]", "");
						pageText = pageText.replaceAll("\\n+", " ");
						pageText = pageText.trim().replaceAll(" +", " ");
						pageText = pageText.toLowerCase();

						// Conver to string list
						List<String> wordList = Arrays.asList(pageText.split(" "));
						// Remove non-meaning words and stopwords
						List<String> cleanedWordList = new ArrayList();
						String cleanedLine = "";
						int wordCount = 0;
						for (String word : wordList) {
							if(dictList.contains(word) && !stopwordList.contains(word)) {
								// Check for duplication
								if (!cleanedWordList.contains(word)) {
									cleanedWordList.add(word);
									cleanedLine += word + " ";
									wordCount++;
								}
							}  
						}

						// Write into cleaned dataset 01
						// Skipp all pages with less than 3 words
						if (cleanedLine != "" && wordCount >= featureThreshold) {
							bw.write(cleanedLine.trim());
							bw.newLine();
							tupleSize++;
							System.out.println("Article "+ ++articleTotal +": " + cleanedLine);

							// Check and add into feature list
							for (String word : cleanedWordList) {
								if (!featureList.contains(word)) {
									featureList.add(word);
									// Write new feature into feature list file
									bw2.write(word);
									bw2.newLine();
								}
							}
						}}
					}
				}
			}
		
			bw.flush();
			bw.close();
			bw2.flush();
			bw2.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Finished cleaning data.");
	}

	// Function: format Data
	static void formatData(String finalFile) throws IOException {
			// Create final oataset
			BufferedWriter bw2 = new BufferedWriter(new FileWriter(finalFile));
			// Read the cleaned dataset
			BufferedReader br2 = new BufferedReader(new FileReader(cleanedDatasetFile));
			// Get size of feature list
			int featureSize = featureList.size();
			
			String line;
			while ((line = br2.readLine()) != null) {
				// Trim line first
				String[] lineArr = line.split(" ");
				// Init an empty binary tuple 
				int[] tuple = new int[featureSize];

				// Replace by 1 for existing features
				for(int i = 0; i < lineArr.length; i++) {
					int featureIdx = featureList.indexOf(lineArr[i]);
					// Existing in the feature list
					if (featureIdx != -1) {
						tuple[featureIdx] = 1;
					} else 
						System.out.println("feature not exist in feature list!!!");
				}

				// Convert to string
				String tupleStr = "";
				for(int i = 0; i < featureSize; i++) {
					tupleStr += tuple[i];
				}

				// Write into final dataset file
				bw2.write(tupleStr);
				bw2.newLine();
			}

			bw2.flush();
			bw2.close();
	}

	// Function: load feature list
	static void loadFeatureList() throws IOException {
		// Read stopWord list
	 	BufferedReader br = new BufferedReader(new FileReader(featureFile));
	 	String line;
	 	while((line = br.readLine()) != null) {
			featureList.add(line);
	 	}
		br.close();
	}

	// Function: load dictionary
	static void loadDictionary() throws IOException {
		// Read dictionary
		BufferedReader br = new BufferedReader(new FileReader(dict));
		String line;
		while((line = br.readLine()) != null) {
			dictList.add(line);
		}
		br.close();
	 
	 	// Read stopWord list
	 	br = new BufferedReader(new FileReader(stopWordFile));
	 	while((line = br.readLine()) != null) {
			stopwordList.add(line);
	 	}
		br.close();
	}
}
