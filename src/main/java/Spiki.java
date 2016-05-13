import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.AbstractMap;
import scala.Tuple2;


public class Spiki implements Serializable {

	// Global variables
	JavaRDD<Itemset> data;
	Itemset featureList;
	// data size
	int N;
	// feature size
	long Fsize;	

	Spiki(String dataFile, String featureFile) {
		String dataPath = "file:///home/ddoan/Projects/java/phiks/datasets/";
		//String dataset = "hdfs://doan1.cs.ou.edu:9000/user/hduser/phiks/in/" + dataFile;
		String dataset = dataPath + dataFile;
		//String featureListFile = "hdfs://doan1.cs.ou.edu:9000/user/hduser/phiks/in/" + featureFile;
		String featureListFile = dataPath + featureFile;

		// Config
		SparkConf conf = new SparkConf().setAppName("SPIKI");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// Load  dataset from HDFS
		Function<String, Itemset> spliter = new Function<String, Itemset>() {
			public Itemset call(String s) {
				return new Itemset(Arrays.asList(s.split(" ")));
			}
		};
		data = sc.textFile(dataset).map(spliter).cache();
		N = (int)data.count();
		System.out.println("Training data: " + N);
		System.out.println("Training total partitions: " + data.getNumPartitions());

		// Load feature list from HDFS
		featureList = new Itemset(sc.textFile(featureListFile).toArray());
		// Size features
		Fsize = featureList.size();
		System.out.println("Feature list size: " + Fsize);
	}
	
	// Function run
	// Input: k: Miki size
	// Output: Miki of size k
	Itemset run(int k) {
		// Check for valid k size
		if (k < 1 || k >= Fsize) {
			System.err.println("K value is invalid. 0 < k < featureSize");
			return null;
		}
		System.out.println("@@@@@@@@@@@@@@@ Output ##################");
		
		// Seed = CurrentMiki
		// Current MIKI
		Itemset currentMiki = new Itemset();
		// Init miki map	
		FeatureMap mikiMap = new FeatureMap();
		mikiMap.put(new Itemset(), 0);

		// Start timer
		long startTime = System.nanoTime();
		for (int t=1; t<=k; t++) {
			// Get remain features at step t
			Itemset remainFeatures = getRemainFeatures(currentMiki, featureList);
			//System.out.println(t+": Remain features: " + remainFeatures);
			// Get candidate set
			Itemsets candidates = getCandidates(remainFeatures, currentMiki);	
			//System.out.println(t+": Candidates: " + candidates.toString());
			// Generate feature maps
			FeatureMaps featureMaps = generateFeatureMaps(remainFeatures, mikiMap);
			//System.out.println(t+": Feature maps: " + featureMaps.toString());

			// Mapper function
			PairFlatMapFunction<Itemset,Projection2,Integer> pairMapper = new PairFlatMapFunction<Itemset,Projection2,Integer>() {
				public Iterable<Tuple2<Projection2,Integer>> call(Itemset tran) {
					// Get suffix set
					// Get S = T intersect F-X 	
					// Find S = transaction Intersect remainingFeatures
					Itemset S = intersect(tran, remainFeatures); 
					// Prefix projection
					// Find projection of current miki on T
					Itemset mikiProj = intersect(tran, currentMiki);
					//System.out.println(": Miki proj: " + mikiProj.toString());
					// Increase frequency of projections
					List<Tuple2<Projection2,Integer>> result = new ArrayList<>();
					for (String item : S) {
						// Create proj key
						// Retrieve the projection pair of feature
						Itemset projKey = new Itemset(mikiProj);
						projKey.add(item);
						// Emit key,value pair
						// Format: ((itemset,projection),1)
						Projection2 key = new Projection2(item,projKey);
						ItemProj_Freq2 pair = new ItemProj_Freq2(key,1);
						result.add(pair);	
					}
					// Emit list of projections
					return result;	
				}
			};

			// Send Mapper function to each worker and reduce projections for candidate itemsets
			JavaPairRDD<Projection2,Integer> projections = data.flatMapToPair(pairMapper).reduceByKey((a,b) -> a+b);
			//System.out.println("projections = " + projections.collect().toString());
			// Update hash maps	
			for (Map.Entry prjEntry : projections.collectAsMap().entrySet()) {
				Projection2 prjKey = (Projection2)prjEntry.getKey();
				String item = prjKey._1();	
				//System.out.println("item: " + item);
				int prjValue = (int)prjEntry.getValue();
				// Retrieve the feature map
				FeatureMap featureMap = featureMaps.get(item);
				// Increase frequency
				featureMap.put(prjKey._2(), prjValue);
				//System.out.println(featureMaps.get(item).toString());
			}
			// Update miki
			// Get freqency of feature.0 projections end 0
			// p.0 = p - p.1
			updateFeatureMaps(featureMaps, mikiMap, N);
			//System.out.println(t+": feature maps: " + featureMaps.toString());
			//System.out.println("before miki: " + currentMiki.toString());
			mikiMap = updateCurrentMiki(featureMaps, currentMiki, N);
			// End timer
			long elapsedTime = System.nanoTime() - startTime;
			double elapsedSeconds = (double)elapsedTime / 1000000000.0;
			System.out.println(t+":@@@@@@@ Current miki: " + currentMiki.toString());
			System.out.println(t+":@@@@@@@ Elapsed Time: " + elapsedSeconds + " seconds.");
		}
		return currentMiki;
	}

	// update Current Miki function
	FeatureMap updateCurrentMiki(FeatureMaps featureMaps, Itemset currentMiki, int splitSize) {
		if (featureMaps == null) {
			System.err.println("ERROR: feature maps is null");
			return null;
		}
		if (currentMiki == null) {
			System.err.println("ERROR: current miki is null");
			return null;
		}

		double maxEntropy = 0.0;	
		String candidate = null;
		//System.out.println("miki feature maps: " + featureMaps.toString());
		for (Map.Entry entry : featureMaps.entrySet()) {
			FeatureMap featureMap = (FeatureMap) entry.getValue(); 
			String feature = (String) entry.getKey();
			double entropy = computeJointEntropy(feature,featureMaps, splitSize); 
			if (maxEntropy < entropy){
				maxEntropy = entropy;
				candidate = feature;
			}
			//System.out.println(feature + ": entropy = " + entropy);
		}
		// Update current miki
		currentMiki.add(candidate);
		return featureMaps.get(candidate); 
	}

	// Function: compute joint entropy
	double computeJointEntropy(String candidate,FeatureMaps featureMaps, int splitSize) {
		FeatureMap candProjs = featureMaps.get(candidate);
		double entropy = 0.0;
		//System.out.println("miki cand projs: " + candProjs.values().toString());
		//System.out.println("miki split size: " + splitSize);
		for (int i : candProjs.values()) {
			if (i > 0) {
				double prob = (double)i/splitSize;
				entropy -= prob*Math.log(prob);
			}
		}
		return entropy;
	}

	// Function: update feature maps
	void updateFeatureMaps(FeatureMaps featureMaps, FeatureMap mikiMap, int splitSize) {
		if (featureMaps == null) {
			System.err.println("ERROR: feature maps is null");
			return; 
		}
		if (mikiMap == null) {
			System.err.println("ERROR: current miki is null");
			return;
		}

		for (Map.Entry entry : featureMaps.entrySet()) {
			FeatureMap featureMap = (FeatureMap) entry.getValue(); 
			String feature = (String) entry.getKey();
			int idx = 0;
			// Loop throuh projection of current miki
			for (Map.Entry projEntry : featureMap.entrySet()) {
				// Only loop through p.1 proj
				if (idx++ % 2 != 0) 
					continue;
				// Keys for p.1
				Itemset projKey1 = (Itemset) projEntry.getKey(); 
				// Key for p.0
				Itemset projKey0 = new Itemset(projKey1);
				projKey0.remove(feature);
				// Check if projKey0 is valid or not
				// If not, skip
				if(featureMap.containsKey(projKey0) == false) {
					continue;
				}

				// Value for p.0
				//System.out.println("1="+projEntry.getValue()+", 2=" +  (int) mikiMap.get(projKey0) + ", 3=" + splitSize + ", 4=" + mikiMap.size());
				int projValue0 = (int) projEntry.getValue();
				if (mikiMap.size() > 1 && mikiMap.get(projKey0) != null) {
					//System.out.println("projKey0: " + projKey0);
					//System.out.println("projValue0: " + (int) mikiMap.get(projKey0));
					projValue0 = (int) mikiMap.get(projKey0) - projValue0; 
				} else {
					projValue0 = splitSize - projValue0;
				}
				// Update value for p.0
				featureMap.put(projKey0, projValue0);
				//System.out.println("Updated feature map: " + featureMap.toString());
			}
			// Update
			featureMaps.put(feature,featureMap);
		}
	}

	// Function: generate hash maps
	FeatureMaps generateFeatureMaps(Itemset remainFeatures, FeatureMap mikiMap) {
		FeatureMaps maps = new FeatureMaps();
		for (String feature : remainFeatures) {
			FeatureMap map = new FeatureMap();
			// Generate candidate's projections
			for (Itemset proj : mikiMap.keySet()) {
				//System.out.println("miki proj: " + proj.toString());
				// Add current miki projection with end .1 by item s
				Itemset newProj = new Itemset(proj);
				newProj.add(feature);
				map.put(newProj, 0);
				// Add current miki projection with end .0 by item s into hash map of s
				map.put(proj, 0);	
				//System.out.println("miki map: " + map.toString());
			}
			//System.out.println("Map: " + map.toString());
			maps.put(feature, map);
		}
		//System.out.println("Maps: " + maps.toString());
		return maps;
	}

	// Function: get remain features
	Itemset getRemainFeatures(Itemset currentMiki, Itemset featureList) {
		// Get remain features 
		Itemset remainFeatures = new Itemset();
		for (String feature : featureList) {
			if (!currentMiki.contains(feature)) {
				remainFeatures.add(feature);	
			}
		}
		return remainFeatures;
	}

	// Function: get candidate itemsets
	Itemsets getCandidates(Itemset remainFeatures, Itemset currentMiki) {
		// Combine each remain feature with current miki to create new candidate
		Itemsets candidates = new Itemsets();
		for (String feature : remainFeatures) {
			Itemset candidate = new Itemset(currentMiki);
			candidate.add(feature);
			candidates.add(candidate);
		}
		return candidates;
	}

	// Function: find interesect set of two itemsets
	Itemset intersect(Itemset list1, Itemset list2) {
		Itemset list = new Itemset();

		for (String t : list1) {
			if(list2.contains(t)) {
				list.add(t);
			}
		}
		return list;
	}
}
