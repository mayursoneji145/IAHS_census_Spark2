package spark.testproject;

import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Population {
	public static void main(String[] args){
		
//Setting the log level to only Error

	Logger.getLogger("org").setLevel(Level.ERROR);


//Initializing the Spark Configuration and setting up Spark Context.

	SparkConf conf = new SparkConf().setAppName("Highest Lowest Population");

	@SuppressWarnings("resource")
	JavaSparkContext sc = new JavaSparkContext(conf);


// Reading the file into a RDD

	JavaRDD<String> popdetails = sc.textFile(args[0]);


//Creating a nested Tuple to hold 2 values; viz, State and Population

	JavaPairRDD<String, Double> popmap = popdetails.mapToPair(

		x -> {
			String[] vals = x.split(",");
			
			return new Tuple2<String,Double>
	                                    (vals[1], Double.parseDouble(vals[9]));
			 }

		);


//ReduceByKey Action on the above RDD

	JavaPairRDD<String,Double> popred
	                        = popmap.reduceByKey((x,y) ->  (x+y));


// Sorting it in Descending and retrieving the top value 

	List<Tuple2<String,Double>> Highestpop = popred.top(1, new TupleSorterD()); 


//Printing the State with Highest Population
	System.out.println("");
	System.out.println("-------------------- State with Highest Population --------------------");
	System.out.println("");
	
	for (Tuple2<String,Double> t : Highestpop ) 
		{
			System.out.println("State: "+t._1+" Population: "+t._2); 
		}

	System.out.println("");
	
// Sorting it in Ascending and retrieving the top value 

	List<Tuple2<String,Double>> Lowestpop = popred.top(1, new TupleSorterA()); 


//Printing the State with Lowest Population
	System.out.println("");
	System.out.println("-------------------- State with Lowest Population --------------------");
	System.out.println("");
	
	for (Tuple2<String,Double> t : Lowestpop ) 
		{
			System.out.println("State: "+t._1+" Population: "+t._2); 
		}
	sc.stop();
	}
}