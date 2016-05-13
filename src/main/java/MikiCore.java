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

// Pair <Itemset, Projeciton>
class Projection extends Tuple2<Itemset, Itemset> {
	Projection(Itemset is, Itemset prj) {
		super(is,prj);
	}
}

// Pair <Projection, Frequency>
class ProjFreq extends Tuple2<Itemset,Integer> {
	ProjFreq(Itemset prj, Integer freq) {
		super(prj,freq);
	}
}

// Pair <Itemset, Entropy>
class ItemEnt extends Tuple2<Itemset, Double> {
	ItemEnt(Itemset is, Double ent) {
		super(is,ent);
	}
	ItemEnt(Tuple2<Itemset,Double> tup) {
		super(tup._1(), tup._2());
	}
}

// Pair <<Itemset, Projeciton>, Frequency>
class ItemProj_Freq extends Tuple2<Projection,Integer> {
	ItemProj_Freq(Projection ip, Integer freq) {
		super(ip,freq);
	}
}

// Pair <<Feature,Projection>,Frequency>
class ItemProj_Freq2 extends Tuple2<Projection2,Integer> {
	ItemProj_Freq2(Projection2 ip, Integer freq) {
		super(ip,freq);
	}
}

// Pair <Itemset,List<<Projection, Frequency>>>
class Item_ProjFreqs extends Tuple2<Itemset,List<ProjFreq>> {
	Item_ProjFreqs(Itemset is, List<ProjFreq> pf) {
		super(is, pf);
	}
}

// Pair <Feature, Projeciton>
class Projection2 extends Tuple2<String, Itemset> {
	Projection2(String is, Itemset prj) {
		super(is,prj);
	}
}

// Hash Map <Projection, Frequency>
class FeatureMap extends LinkedHashMap<Itemset,Integer> {
	FeatureMap() {
		super();
	}
}

// Map of Hash map <Feature, HashMap>
class FeatureMaps extends HashMap<String, FeatureMap> {
	FeatureMaps() {
		super();
	}
}
