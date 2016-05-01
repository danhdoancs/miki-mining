import scala.Tuple2;
import java.util.AbstractMap;

public class ItemProj extends Tuple2<Itemset, Projection> {
	ItemProj(Itemset is, Projection prj) {
		super(is,prj);
	}
}
