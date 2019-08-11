package spark.testproject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class Household {
	public static void main(String[] args){
		
//Setting the log level to only Error
		Logger.getLogger("org").setLevel(Level.ERROR);
		
		
//Initializing the Spark Configuration and setting up Spark Context.
		SparkConf conf =new SparkConf().setAppName("Fertility Rate Bihar");

		@SuppressWarnings("resource")
		JavaSparkContext sc = new JavaSparkContext(conf);

		
//Reading the file into a RDD
		JavaRDD<String> hhdetails  = sc.textFile(args[0]);
	
		
//Creating a nested Tuple to hold 3 values; viz, State, HH Size and 1(for the number of occurence purpose)
		JavaPairRDD<String, Tuple2<Double,Integer>> hhmap = hhdetails.mapToPair(

				x -> {
					String[] vals = x.split(",");
					return new Tuple2<String,Tuple2<Double,Integer>>
			                                    (vals[1], new Tuple2<Double, Integer>(Double.parseDouble(vals[27]), 1));
						}

					);
		
		
//ReduceByKey Action on the above RDD
		JavaPairRDD<String, Tuple2<Double, Integer>> hhred = hhmap.reduceByKey(

				(tuple1,tuple2) ->  new Tuple2<Double, Integer>
                              (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2)

				);
		
		
//Getting the Average

		JavaPairRDD<String, Double> hhAvg = hhred.mapToPair(
				
					tuple1 -> new Tuple2<String, Double>
		            		(tuple1._1, tuple1._2._1/tuple1._2._2)
				);
		
		System.out.println("");
		
		
//Sorting it in Descending and retrieving the top value 

		List<Tuple2<String,Double>> Highesthhavg = hhAvg.top(1, new TupleSorterD()); 


//Printing the Highest Household 

		for (Tuple2<String,Double> t : Highesthhavg ) 
			{
				System.out.println("State: "+t._1+"    Highest Average Household: "+t._2); 
			}

		System.out.println("");
		
		
//Sorting it in Ascending and retrieving the top value 

		List<Tuple2<String,Double>> Lowesthhavg = hhAvg.top(1, new TupleSorterA()); 


//Printing the Lowest Household 

		for (Tuple2<String,Double> t : Lowesthhavg ) 
			{
				System.out.println("State: "+t._1+"    	Lowest Average Household: "+t._2); 
			}		
		
		System.out.println("");
		
		sc.stop();
	}
}
