import scala.Tuple2;
import java.util.AbstractMap;

public class ProjFreq extends Tuple2<Projection, Integer> {
	ProjFreq(Projection prj, Integer freq) {
		super(prj,freq);
	}
}
