package spark.testproject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class FertilityBihar {

	public static void main(String[] args) {

//Setting the log level to only Error

		Logger.getLogger("org").setLevel(Level.ERROR);


//Initializing the Spark Configuration and setting up Spark Context.

		SparkConf conf = new SparkConf().setAppName("Fertility- Bihar");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);

		
//Filtering out data related to Bihar

		JavaRDD<String> fertilitybihar = sc.textFile(args[0]).map(
			x-> {	
				String[] vals = x.split(",");

					if (vals[1].equals("Bihar")){
						return x;
					}
					else return "";
			    }
		);
	
		
		JavaRDD<String> filterbih = fertilitybihar.filter(row-> row!="");
		
		
//Creating a Tuple to hold 3 values; viz, State, Fertility and 1

		JavaPairRDD<String, Tuple2<Double,Integer>> bihmap = filterbih.mapToPair(

				x -> {
					String[] vals = x.split(",");
					return new Tuple2<String,Tuple2<Double,Integer>>
			                                    (vals[1], new Tuple2<Double, Integer>(Double.parseDouble(vals[297]), 1));
					 }

		);

		
//ReduceByKey Action on the above RDD

		JavaPairRDD<String, Tuple2<Double, Integer>> bihred = bihmap.reduceByKey(

		       (tuple1,tuple2) ->  new Tuple2<Double, Integer>
		                         (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)

		);

		
//Getting the Average

		JavaPairRDD<String, Double> bihfert = bihred.mapToPair(    

					tuple1 -> new Tuple2<String, Double>
		                         (tuple1._1, tuple1._2._1/tuple1._2._2)

		);



//Printing the Bihar's Fertility Rate

		System.out.println("");

		String ky = String.valueOf(bihfert.keys().collect());
		String vl = String.valueOf(bihfert.values().collect());

		System.out.println("State: " + ky + "	Fertility Rate :  " + vl); 
		
		System.out.println("");
		
	sc.stop();

	}
}