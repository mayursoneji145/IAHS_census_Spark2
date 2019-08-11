package spark.testproject;

import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;

@SuppressWarnings("serial")
public class TupleSorterD implements Comparator<Tuple2<String,Double>>,Serializable
	{
	@Override
	public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2)
	{
		if(o1._2 < o2._2)
			return -1;
		else if (o1._2 > o2._2)
			return 1;
		else
			return 0;
	}

}