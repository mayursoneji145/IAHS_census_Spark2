package spark.testproject;

import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ChildMortality {
	
	public static void main(String[] args) {
		
//Setting the log level to only Error

		Logger.getLogger("org").setLevel(Level.ERROR);


//Initializing the Spark Configuration and setting up Spark Context.

		SparkConf conf = new SparkConf().setAppName("Child Mortality");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);


//Filtering out data related to Rajasthan

		JavaRDD<String> filterraj = sc.textFile(args[0]).map(
			x-> {	
				String[] vals = x.split(",");

					if (vals[1].equals("Rajasthan")) {
						return x;
					}
					else return "";
			    }
		);

		JavaRDD<String> remnull = filterraj.filter(row-> row!="");

//Creating a Tuple to hold 3 values; viz, State, District and Child Mortality

		JavaPairRDD<String, Double> rajm2p = remnull.mapToPair(

			x -> {
				String[] vals = x.split(",");

					return new Tuple2<String,Double>
		                                    (vals[2],Double.parseDouble(vals[606]));
				 }

		);


		List<Tuple2<String,Double>> HigheschildmortRaj = rajm2p.top(1, new TupleSorterD()); 

		System.out.println("");

//Printing Rajasthan's Child Mortality Rate

		System.out.println("-------------------- Highest Child Mortality Rate for District in Rajasthan --------------------");
		System.out.println("");
		
		for (Tuple2<String,Double> t : HigheschildmortRaj) 
			{
				System.out.println("State: Rajasthan  |  District: "+t._1+"   |  Child Mortality Rate: "+t._2); 
			}
		
		System.out.println("");
		
	sc.stop();
	}
}