import scala.Tuple2;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;

class ItemProj_Freq extends Tuple2<ItemProj,Integer> {
	ItemProj_Freq(ItemProj ip, Integer freq) {
		super(ip,freq);
	}
}

class Transaction extends HashSet<String> {
	Transaction(List<String> row) {
		super(row);
	}
	Transaction() {
		super();
	}
}

class Features extends ArrayList<String> {
	Features(List<String> list) {
		super(list);
	}
	Features() {
		super();
	}
}

class FeatureMap extends LinkedHashMap<Projection,Integer> {
	FeatureMap() {
		super();
	}
}

class FeatureMaps extends LinkedHashMap<String, FeatureMap> {
	
	FeatureMaps() {
		super();
	}
}

class Itemset extends HashSet<String> {
	Itemset(Itemset is) {
		super(is);
	}
	Itemset() {
		super();
	}	
}

class Projection extends HashSet<String> {
	Projection(Projection prj) {
		super(prj);
	}
	Projection() {
		super();
	}
}

class ItemEnt extends Tuple2<Itemset, Double> {
	ItemEnt(Itemset is, Double ent) {
		super(is,ent);
	}
}
