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

public class q2Main {
	 
	

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {

		//String logFileIliad = "/home/karunsh/workspace/RecomSystem/Files/iliad/part-0000[0-4]*";
		//String logFileOdyssey = "/home/karunsh/workspace/RecomSystem/Files/odyssey/part-0000[0-4]*";
		String logFileIliad = args[0];
		String logFileOdyssey = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);
		

		JavaRDD<String> sessionCount_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "achille");
		JavaRDD<String> sessionCount_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "achille");

		// Print Question 2
		System.out.println("Question 2 Sessions of user 'achille'\n + Iliad :" + sessionCount_Iliad.count() + "\n"
				+ "+ Odyssey :" + sessionCount_oddyssey.count());

	}

	
	public static JavaRDD<String> sessionDetaililliad(JavaRDD<String> logRDDIllad, final String Session, final String user) {
		JavaRDD<String> sessionCount = logRDDIllad.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			
			public Boolean call(String sessionC) throws Exception {
				// TODO Auto-generated method stub
				return (sessionC.contains(Session)) ? true : false;
			}
		});
		JavaRDD<String> sessionCount1 = sessionCount.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			
			public Boolean call(String sessionC) throws Exception {
				// TODO Auto-generated method stub
				return (sessionC.contains(user)) ? true : false;
			}
		});
		return sessionCount1;
	}

}