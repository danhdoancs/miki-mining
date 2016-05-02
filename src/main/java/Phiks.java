import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.AbstractMap;
import scala.Tuple2;

import com.google.common.collect.Lists;
import java.io.FileWriter;

public class Phiks implements Serializable {

	JavaRDD<Transaction> data;
	Features featureList;
	int N;
	long Fsize;	
	int k;
	long startTime;
	
	Phiks(String dataFile, String featureFile, int k) {
		String dataPath = "file:///home/ddoan/Projects/java/phiks/datasets/";
		//String dataset = "hdfs://doan1.cs.ou.edu:9000/user/hduser/phiks/in/" + dataFile;
		String dataset = dataPath + dataFile;
		//String featureListFile = "hdfs://doan1.cs.ou.edu:9000/user/hduser/phiks/in/" + featureFile;
		String featureListFile = dataPath + featureFile;

		SparkConf conf = new SparkConf().setAppName("PHIKS");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);

		// Load training data from HDFS
		Function<String, Transaction> spliter = new Function<String, Transaction>() {
			public Transaction call(String s) {
				return new Transaction(Arrays.asList(s.split(" ")));
			}
		};
		data = sc.textFile(dataset).map(spliter).cache();
		N = (int)data.count();
		System.out.println("Training data: " + N);
		System.out.println("Training total partitions: " + data.getNumPartitions());

		// Load feature list from HDFS
		//System.out.println(featureListFile);
		featureList = new Features(sc.textFile(featureListFile).toArray());
		// Size features
		Fsize = featureList.size();
		System.out.println("Feature list size: " + Fsize);
		// Init k
		this.k = k;
	}
	void run() {
		runJob1();
	}
/*
	Itemset run() {
		// Check k size
		if (k < 1 || k >= Fsize) {
			System.err.println("K value is invalid. 0 < k < featureSize");
			return null;
		}
		// Miki
		ItemEnt miki;
		System.out.println("@@@@@@@@@@@@@@@ Output ##################");
		Tuple2 result = runJob1();
		ItemEnt job1Miki = (ItemEnt)result._1();
		//System.out.println("@@@@@@ Job 1 Miki: " + job1Miki.toString());
		List<List<String>> missingCandidates = (List<List<String>>)result._2();
		System.out.println("!!!!!! Missing candidates: " + missingCandidates.size());
		System.out.println("!!!!! Missing candidates: " + missingCandidates.toString());
		// Run job 2 to re calculate the missing candidates' entropy
		if (!missingCandidates.isEmpty()) {
			//miki = runJob2(missingCandidates, job1Miki);
		} else {
			miki = job1Miki;
		}
		return miki._1();
	}
	*/

	// Job 2
	/*
	Tuple2 runJob2(List<List<String>> missingCandidates, Tuple2<List,Double> job1Miki) {
		Tuple2<List,Double> job2Miki = data.flatMapToPair(tran -> {
				// Init empty result projections
				List<Tuple2<Tuple2,Integer>> projs = new ArrayList<>();
				// Get projection of each missing candidate
				for (List<String> cand : missingCandidates) {
					List<String> proj = intersection(tran, cand);
					Tuple2 tupKey = new Tuple2(cand, proj);
					Tuple2<Tuple2,Integer> tup = new Tuple2(tupKey, 1);
					projs.add(tup);
				}
				return projs;
		})
		// Sum all frequency of each projeciton	of each candidate
		.reduceByKey((a,b) -> a+b)	
		// Combine all projections for each candidate
		// Transform key value format
		.mapToPair(tup -> {
				Tuple2 tupKey = (Tuple2)tup._1();
				List<String> resultKey = (List<String>)tupKey._1();
				Tuple2<List<String>,Integer> resultValue = new Tuple2(tupKey._2(), tup._2());
				List<Tuple2> resultValues = new ArrayList<>();
				resultValues.add(resultValue);
				return new Tuple2<List,List>(resultKey,resultValues);
		})
		// Combien projecitons
		.reduceByKey((a,b) -> {
			List listA = (List)a;
			List listB = (List)b;
			listA.addAll(listB);
			return listA;
		})
		// Compute entropy
		.mapToPair(tup -> {
			List cand = (List)tup._1();
			List<Tuple2> projs = (List)tup._2();
			
			double entropy = 0.0;
			for (Tuple2 proj : projs) {
				int freq = (int)proj._2();
				double prob = (double)freq/N;
				entropy -= prob*Math.log(prob);
			}
			return new Tuple2<List,Double>(cand, entropy);
		})
		// Get the global MIKI
		.reduce((a,b) -> {
			Tuple2<List,Double> tupA = (Tuple2<List,Double>)a;
			Tuple2<List,Double> tupB = (Tuple2<List,Double>)b;
			return tupA._2() > tupB._2() ? tupA : tupB;
		});
		
		// Select miki from job1 and job2
		Tuple2 miki = job1Miki != null && job1Miki._2() > job2Miki._2() ? job1Miki : job2Miki;
		// End timer
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedSeconds = (double)elapsedTime / 3000000000.0;
		System.out.println("@@@@@@@ Job 2: Global miki: " + job2Miki.toString());
		System.out.println("@@@@@@@ Job 2: Elapsed Time: " + elapsedSeconds + " seconds.");
		System.out.println("@@@@@@@ Final miki: " + miki.toString());
		return miki;
	}
*/

	// Job 1
	void runJob1() {
		// Start timer
		startTime = System.nanoTime();
		// Work on each partition
		JavaRDD<ItemProj_Freq> projections = data.mapPartitions(new FlatMapFunction<Iterator<Transaction>,ItemProj_Freq>() {
				public Iterable<ItemProj_Freq> call(Iterator<Transaction> tranIt) {
					// Cache partition into memory as ArrayList
					List<Transaction> subset = Lists.newArrayList(tranIt);
					int localN = subset.size();
					System.out.println(": Subset size: " + subset.size());

					// Init result pair 
					List<ItemProj_Freq> result = new ArrayList<>();
					// Init 2 core variables to keep track of local miki
					// Current MIKI
					Itemset localMiki = new Itemset();
					// Init miki map	
					FeatureMap mikiMap = new FeatureMap();
					mikiMap.put(new Projection(), 0);
					// Loop k times to find local MIKI
					for (int t=1; t<=k; t++) {
					// Get remain features;
					Features remainFeatures = getRemainFeatures(localMiki, featureList);
					//System.out.println(t+": Remain features: " + remainFeatures);
					// Get candidate set
					Itemsets candidates = getCandidates(remainFeatures, localMiki);	
					//System.out.println(t+": Candidates: " + candidates.toString());
					// Generate feature maps
					FeatureMaps featureMaps = generateFeatureMaps(remainFeatures, mikiMap);
					//System.out.println(t+": Feature maps: " + featureMaps.toString());
					// Scan the data split
					// For each transaction T, get S = T intesect F/X
					for (Transaction tran : subset) {
						// Get S = T intersect F-X 	
						// Find S = transaction Intersect remainingFeatures
						Features S = (Features)intersect(tran, remainFeatures); 
						//System.out.println(": Transaction: " + tran.toString());
						//System.out.println(": S " + S.toString());
						// Find projection of current miki on T
						Projection mikiProj = (Projection)intersect(tran, localMiki);
						//System.out.println(": Miki proj: " + mikiProj.toString());
						// Increase frequency of projections
						for (String item : S) {
							// Create feature key 
							Projection key = new Projection();
							key.add(item);
							// Retrieve the projection pair of feature
							Projection projKey = new Projection(mikiProj);
							projKey.add(item);
							// Retrieve the feature map
							FeatureMap featureMap = featureMaps.get(item);
							// Increase frequency
							Integer projValue = featureMap.get(projKey) == null ? 0 : featureMap.get(projKey);
							featureMap.put(projKey,projValue + 1);
							//System.out.println(featureMaps.get(item).toString());
						}
					}
					// Update miki for t = 1 -> k-1
					// Get freqency of feature.0 projections end 0
					// p.0 = p - p.1
					updateFeatureMaps(featureMaps, mikiMap, localN);
					//System.out.println(t+": feature maps: " + featureMaps.toString());
					//System.out.println("before miki: " + localMiki.toString());
					if (t < k) {
						mikiMap = updateCurrentMiki(featureMaps, localMiki, localN);
						long elapsedTime = System.nanoTime() - startTime;
						double elapsedSeconds = (double)elapsedTime / 1000000000.0;
						System.out.println("@@@@@@@ Job 1: Local miki: " + localMiki.toString());
						System.out.println("@@@@@@@ Job 1: Progress Time: " + elapsedSeconds + " seconds.");
					} else {
						// Compute result
					// Emit key,value pair at last step k
					// Format: ((itemset,projection),1)
					for (Map.Entry entry : featureMaps.entrySet()) {
						FeatureMap featureMap = (FeatureMap) entry.getValue();
						// Get key as candidate
						String feature = (String) entry.getKey();
						Itemset candidate = new Itemset(localMiki);
						candidate.add(feature);
						// Loop through each projection of candidate in feature map
						for (Map.Entry projEntry : featureMap.entrySet()) {
							Projection projKey = (Projection) projEntry.getKey();
							int projValue = (int) projEntry.getValue();
							// Skip projection with 0 frequency
							if (projValue == 0)
								continue;
							// Add into result
							ItemProj resultKey = new ItemProj(candidate,projKey);
							ItemProj_Freq resultPair = new ItemProj_Freq(resultKey,projValue);
							result.add(resultPair);   
						}
					}
					}	
				} // End loop t = 1 -> k
					return result;
			} // End FlatMapFunction
		}); // End map fucntion
		/*
		//System.out.println(projections.collect().toString());
		JavaPairRDD pairProj = projections.mapToPair(new PairFunction<ItemProj_Freq, ItemProj, Integer>() {
			public ItemProj_Freq call(ItemProj_Freq tup) {
				return tup;
			}
		}).reduceByKey((a,b) -> a+b);	
		// Reduce
		// Combine to <Candidate, [Projection -> Frequency]>
		JavaPairRDD combinedProj = pairProj.mapToPair(new PairFunction<ItemProj_Freq, Itemset, List<ProjFreq>>() {
			public Item_ProjFreqs call(ItemProj_Freq tup) {
				ItemProj tupKey = (ItemProj)tup._1();
				Itemset resultKey = (Itemset)tupKey._1();
				ProjFreq resultValue = new ProjFreq(tupKey._2(), tup._2());
				List<ProjFreq> resultValues = new ArrayList<>();
				resultValues.add(resultValue);
				return new Item_ProjFreqs(resultKey,resultValues);
			}
		}).reduceByKey((a,b) -> {
			List<ProjFreq> listA = (List<ProjFreq>)a;
			List<ProjFreq> listB = (List<ProjFreq>)b;
			listA.addAll(listB);
			return listA;
		});

		//System.out.println(combinedProj.collect().toString());
		// Compute entropy or -1 if missing projection
		JavaPairRDD entropies = combinedProj.mapToPair(new PairFunction<Item_ProjFreqs, Itemset, Double>() {
			public ItemEnt call(Item_ProjFreqs tup) {
				Itemset candidate = (Itemset)tup._1();
				List<ProjFreq> projs = (List<ProjFreq>)tup._2();
				double entropy = 0.0;
				int totalFreq = 0;
				for (ProjFreq entry : projs) {
					int freq = (int)entry._2();
					totalFreq += freq;
					if (freq > 0) {
					double prob = (double)freq/N;
						entropy -= prob*Math.log(prob);
					}
				}
				// Check if the candidate missed any projections
				if (totalFreq < N) {
					// Compute upper bound
					// Set upper bound entropy as negative value to seperate with filled candidates
					entropy = -1; 
				}
				return new ItemEnt(candidate, entropy);
			}
		});
		
		// Work on reducer
		// Get missing candidates
		JavaPairRDD missingCandidatesPairRdd = entropies.filter(a -> (Double)((Tuple2)a)._2() < 0);
		JavaRDD<Itemset> missingCandidatesRdd = missingCandidatesPairRdd.keys();
		Itemsets missingCandidates = (Itemsets)missingCandidatesRdd.collect();
		// Get the global MIKI
		JavaPairRDD filteredCandidates = entropies.filter(a -> (Double)((Tuple2)a)._2() > -1);
		ItemEnt miki = null;
		if (filteredCandidates.count() > 0) {
				miki = (ItemEnt)filteredCandidates.reduce((a,b) -> {
				ItemEnt tupA = (ItemEnt)a;
				ItemEnt tupB = (ItemEnt)b;
				return tupA._2() > tupB._2() ? tupA : tupB;
			});
			// Compute upper bound entropies
		//	List<Tuple2<List<String>,List<Map.Entry<List<String>,Integer>>>> missingCands = missingCandidatesPairRdd.collect();		
		//	List<Tuple2<List<String>,List<Map.Entry<List<String>,Integer>>>> filterCands = filteredCandidates.collect();		
		//	for (Tuple2<List<String>,List<Map.Entry<List<String>,Integer>>> missingCand : missingCands) {
		//		 double ubEntropy = getUpperBoundEntropy(missingCand, filterCands);	
		//	}	
		}
		// End timer
		long elapsedTime = System.nanoTime() - startTime;
		double elapsedSeconds = (double)elapsedTime / 1000000000.0;
		System.out.println("@@@@@@@ Job 1: Global miki: " + (miki != null ? miki.toString() : "Empty"));
		System.out.println("@@@@@@@ Job 1: Elapsed Time: " + elapsedSeconds + " seconds.");
		return new Tuple2<ItemEnt,Itemsets>(miki, missingCandidates);
		*/
	}

	// compute upper bound entropy
	double getUpperBoundEntropy(Tuple2<List<String>,List<Map.Entry<List<String>,Integer>>> candTup, List<Tuple2<List<String>,List<Map.Entry<List<String>,Integer>>>> filteredCands) {
		List<String> cand = candTup._1();
		List<Map.Entry<List<String>,Integer>> projs = candTup._2();
		Set<List<String>> candSubSets = subSets(cand);
		//List<List<String>> candSubSets = subSets(cand).toArray();
		//System.out.println("Subset: " + candSubSets.toString());
		// Find the biggest subset of cand
		List<String> biggestSubset;
		Iterator<List<String>> subsetIt = candSubSets.iterator();
		while (subsetIt.hasNext()) {
			biggestSubset = subsetIt.next();
			// Find frequency of 
			for (Tuple2<List<String>,List<Map.Entry<List<String>,Integer>>> filledCandProj : filteredCands) {
				List<String> fillCand = filledCandProj._1();
				// Check if the subset is in any filled candidate set
				if (fillCand.containsAll(fillCand)) {
					// Yes, now get the projections for the subset using the filled candidate
					Map<List<String>,Integer> subsetProjs = new LinkedHashMap<>();
					for (Map.Entry<List<String>,Integer> proj : filledCandProj._2()) {
						List<String> projSet = proj.getKey();
						int projFreq = proj.getValue();
						List<String> subsetProj = intersection(projSet, biggestSubset);
						//System.out.println(subsetProj.toString());
						if (subsetProjs.containsKey(subsetProj)) {
							subsetProjs.put(subsetProj,subsetProjs.get(subsetProjs) + projFreq);	
						} else {
							subsetProjs.put(subsetProj, projFreq);
						}
					}	
					System.out.println("############## " + subsetProjs.toString());
					break;	
				}	
			}	
		}
		return 1.0;
	}

	<T> Set<List<T>> subSets(List<T> cand) {
		Set<List<T>> result = new LinkedHashSet<>(); 
		if (cand.size() == 1) {
			 return result;
		}

		List<List<T>> newCands = new ArrayList<>();
		for (T item : cand) {
			List<T> withoutItem = removeItem(cand, item);	
			newCands.add(withoutItem);
			result.add(withoutItem);
		}

		for (List<T> newCand : newCands) {
			result.addAll(subSets(newCand));
		}
		return result;
	}

	Map<List<String>,Integer> updateCurrentMiki(Map<String,Map<List<String>,Integer>> featureMaps, List<String> localMiki, int splitSize) {
		if (featureMaps == null) {
			System.err.println("ERROR: feature maps is null");
			return null;
		}
		if (localMiki == null) {
			System.err.println("ERROR: current miki is null");
			return null;
		}

		double maxEntropy = 0.0;	
		String candidate = null;
		//System.out.println("miki feature maps: " + featureMaps.toString());
		for (Map.Entry entry : featureMaps.entrySet()) {
			Map<List<String>,Integer> featureMap = (Map<List<String>,Integer>) entry.getValue(); 
			String feature = (String) entry.getKey();
			double entropy = computeJointEntropy(feature,featureMaps, splitSize); 
			if (maxEntropy < entropy){
				maxEntropy = entropy;
				candidate = feature;
			}
			//System.out.println(feature + ": entropy = " + entropy);
		}
		// Update current miki
		localMiki.add(candidate);
		return featureMaps.get(candidate); 
	}

	double computeJointEntropy(String candidate,Map<String,Map<List<String>,Integer>> featureMaps, int splitSize) {
		Map<List<String>,Integer> candProjs = featureMaps.get(candidate);
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
				Projection projKey1 = (Projection) projEntry.getKey(); 
				// Key for p.0
				Projection projKey0 = new Projection(projKey1);
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

	FeatureMaps generateFeatureMaps(Features remainFeatures, FeatureMap mikiMap) {
		FeatureMaps maps = new FeatureMaps();
		for (String feature : remainFeatures) {
			FeatureMap map = new FeatureMap();
			// Generate candidate's projections
			for (Projection proj : mikiMap.keySet()) {
				//System.out.println("miki proj: " + proj.toString());
				// Add current miki projection with end .1 by item s
				Projection newProj = new Projection(proj);
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

	Features getRemainFeatures(Itemset localMiki, Features featureList) {
		// Get remain features 
		Features remainFeatures = new Features();
		for (String feature : featureList) {
			if (!localMiki.contains(feature)) {
				remainFeatures.add(feature);	
			}
		}
		return remainFeatures;
	}

	Itemsets getCandidates(Features remainFeatures, Itemset localMiki) {
		// Combine each remain feature with current miki to create new candidate
		Itemsets candidates = new Itemsets();
		for (String feature : remainFeatures) {
			Itemset candidate = new Itemset(localMiki);
			candidate.add(feature);
			candidates.add(candidate);
		}
		return candidates;
	}

	Set<String> intersect(Set<String> list1, Set<String> list2) {
		Set<String> list = new LinkedHashSet<String>();

		for (String t : list1) {
			if(list2.contains(t)) {
				list.add(t);
			}
		}
		return list;
	}
	List<String> intersection(List<String> list1, List<String> list2) {
		List<String> list = new ArrayList<String>();

		for (String t : list1) {
			if(list2.contains(t)) {
				list.add(t);
			}
		}
		return list;
	}

	<T> List<T> removeItem(List<T> cand, T item) {
		List<T> list = new ArrayList<T>();
		for (T t : cand) {
			if(!t.equals(item)) {
				list.add(t);
			}
		}
		return list;
	}
}
