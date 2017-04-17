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

public class q4Main {

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {

		// String logFileIliad =
		// "/home/karunsh/workspace/RecomSystem/Files/iliad/part-0000[0-4]*";
		// String logFileOdyssey =
		// "/home/karunsh/workspace/RecomSystem/Files/odyssey/part-0000[0-4]*";
		String logFileIliad = args[0];
		String logFileOdyssey = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);

		JavaRDD<String> sessionDetail_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "user");
		JavaRDD<String> sessionDetail_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "user");

		// Q-4
		JavaPairRDD<String, Integer> rddeUserSessionCount_Iliad = getSessionCountwithUser(sessionDetail_Iliad)
				.sortByKey(true);
		JavaPairRDD<String, Integer> rddeUserSessionCount_Oddyssey = getSessionCountwithUser(sessionDetail_oddyssey)
				.sortByKey(true);

		System.out.println("* Q4: sessions per user  \n + Iliad :" + rddeUserSessionCount_Iliad.collect()
				+ "\n + Odyssey :" + rddeUserSessionCount_Oddyssey.collect());

	}

	public static JavaRDD<String> getSeesionUser(JavaRDD<String> sessionDetail_Iliad) {
		JavaRDD<String> rddUserIliad = sessionDetail_Iliad.flatMap(new FlatMapFunction<String, String>() {
			
			public Iterator<String> call(String s) {

				String sysUser = getSessionUser(s);
				return (Iterator<String>) Arrays.asList(sysUser).iterator();
			}

			public String getSessionUser(String s) {
				int indexOfUser;
				int endIndex;
				String userName = null;
				indexOfUser = (s.indexOf("user"));
				endIndex = s.indexOf(".");
				userName = s.substring(indexOfUser + 5, endIndex);
				return userName;
			}
		});
		return rddUserIliad;
	}

	/**
	 * @param sc
	 * @param sessionUserIliad
	 * @return
	 */
	public static JavaPairRDD<String, Integer> getSessionCountwithUser(JavaRDD<String> sessionDetail) {
		JavaRDD<String> rddSessionUser_Iliad = getSeesionUser(sessionDetail);
		JavaPairRDD<String, Integer> rddeUserSessionmap = rddSessionUser_Iliad
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);

					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					public Integer call(Integer i1, Integer i2) throws Exception {
						// TODO Auto-generated method stub
						return i1 + i2;
					}
				});
		return rddeUserSessionmap;
	}

	public static JavaRDD<String> sessionDetaililliad(JavaRDD<String> logRDDIllad, final String Session,
			final String user) {
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