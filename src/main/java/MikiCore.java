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

class Item_ProjFreqs extends Tuple2<Itemset,List<ProjFreq>> {
	Item_ProjFreqs(Itemset is, List<ProjFreq> pf) {
		super(is, pf);
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

class Features extends HashSet<String> {
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

class ItemProj extends Tuple2<Itemset, Projection> {
	ItemProj(Itemset is, Projection prj) {
		super(is,prj);
	}
}

class Itemsets extends ArrayList<Itemset> {
}

class ProjFreq extends Tuple2<Projection,Integer> {
	ProjFreq(Projection prj, Integer freq) {
		super(prj,freq);
	}
}
