import scala.Tuple2;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.HashMap;

// Set of item, feature
// Itemset, Transaction, Features, Projection
class Itemset extends HashSet<String> {
	Itemset(List<String> is) {
		super(is);
	}
	Itemset(Itemset is) {
		super(is);	
	}

	Itemset() {
		super();
	}	
}
// List of itemsets
class Itemsets extends ArrayList<Itemset> {
	Itemsets(List<Itemset> iss) {
		super(iss);
	}
	Itemsets() {
		super();
	}
}
// <A,B> -> 11, 10, 01, 00
class Projection extends Tuple2<Itemset, Itemset> {
	Projection(Itemset is, Itemset prj) {
		super(is,prj);
	}
}
// 11 -> 3, 10 -> 2
class ProjFreq extends Tuple2<Itemset,Integer> {
	ProjFreq(Itemset prj, Integer freq) {
		super(prj,freq);
	}
}
// <A,B> -> 1.53
class ItemEnt extends Tuple2<Itemset, Double> {
	ItemEnt(Itemset is, Double ent) {
		super(is,ent);
	}
	ItemEnt(Tuple2<Itemset,Double> tup) {
		super(tup._1(), tup._2());
	}
}
class ItemProj_Freq extends Tuple2<Projection,Integer> {
	ItemProj_Freq(Projection ip, Integer freq) {
		super(ip,freq);
	}
}

class Item_ProjFreqs extends Tuple2<Itemset,List<ProjFreq>> {
	Item_ProjFreqs(Itemset is, List<ProjFreq> pf) {
		super(is, pf);
	}
}

class FeatureMap extends LinkedHashMap<Itemset,Integer> {
	FeatureMap() {
		super();
	}
}

class FeatureMaps extends HashMap<String, FeatureMap> {
	FeatureMaps() {
		super();
	}
}
