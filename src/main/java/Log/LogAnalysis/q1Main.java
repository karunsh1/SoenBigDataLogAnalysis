// Question 1 : print total number of available log lines
package Log.LogAnalysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.functions;
import scala.Tuple2;

public class q1Main {
	 
	

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {

		
		String logFileIliad = args[0]; // argument 1 for log file 1
		String logFileOdyssey = args[1]; // argument 2 for log file 2
		
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);
		// print total number of available log lines 
		System.out.println("Question 1 Line Counts\n + Iliad :" + logRDDIllad.count() + "\n" + "+ Odyssey :"
				+ logRDDOdyssey.count());

		
	}
}

	
