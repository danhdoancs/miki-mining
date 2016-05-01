import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.AbstractMap;
import scala.Tuple2;


public class MikiMining {

	public static void main(String[] args) throws Exception {

		// Check input size
		if (args.length < 4) {
			System.err.println("Usage:<algorithm_idx> <data_file> <feature_file> <k>");
			return;
		}

		int algorithmIdx = Integer.parseInt(args[0]);
		String dataFile = args[1];
		String featureFile = args[2]; 
		int k = Integer.parseInt(args[3]);	

		switch (algorithmIdx) {
			case 1:
				// SPIKI
				Spiki spiki = new Spiki(dataFile, featureFile);
				spiki.run(k);
				break;
			case 2:
				// PHIKS
				Phiks phiks = new Phiks(dataFile, featureFile, k);
				phiks.run();
				break;
		}
	}
}
