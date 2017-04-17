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

public class q6Main {

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

		JavaRDD<String> errorRDDIllad = getErrorCount(logRDDIllad, "error");
		JavaRDD<String> errorRDDOdyssey = getErrorCount(logRDDOdyssey, "error");

		// Print Question 6
		List<Tuple2<Integer, String>> most5FrequentMessages_iliad = getTop5Errormessages(sc, errorRDDIllad);
		List<Tuple2<Integer, String>> most5FrequentMessages_Odyssey = getTop5Errormessages(sc, errorRDDOdyssey);
		System.out.println("5 most frequent error messages \n +  Iliad :" + most5FrequentMessages_iliad
				+ "\n + Odyssey :" + most5FrequentMessages_Odyssey);

	}

	public static List<Tuple2<Integer, String>> getTop5Errormessages(JavaSparkContext sc,
			JavaRDD<String> errorRDDIllad) {
		JavaRDD<String> rddErrorList_Iliad1 = sc.parallelize(getErrorMessage(errorRDDIllad));

		JavaPairRDD<String, Integer> rdderrorListMap1 = rddErrorList_Iliad1
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<>(s, 1);

					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer i1, Integer i2) throws Exception {
						// TODO Auto-generated method stub
						return i1 + i2;
					}
				});

		JavaPairRDD<Integer, String> swappedErrorPair = rdderrorListMap1
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
						return item.swap();
					}

				}).sortByKey(false);

		List<Tuple2<Integer, String>> GetTop5Errors = swappedErrorPair.take(5);
		return GetTop5Errors;
	}

	public static List<String> getErrorMessage(JavaRDD<String> errorRDDIllad) {
		List<String> listErrorWithUser = errorRDDIllad.collect();
		// System.out.println(errorRDDIllad.take(10));

		int indexofuser = 0;
		String error = null;
		List<String> errorList = new ArrayList<>();
		for (String s : listErrorWithUser) {
			Pattern pattern = Pattern.compile("^(\\S+) \\d{2} (\\S+) (\\S+) ");
			Matcher matcher = pattern.matcher(s);
			while (matcher.find()) {
				matcher.group();
				indexofuser = matcher.end();

			}
			error = s.substring(indexofuser).trim();
			errorList.add(error);

		}
		return errorList;
	}

	public static JavaRDD<String> getErrorCount(JavaRDD<String> errorLogRDDFile, final String error) {
		return errorLogRDDFile.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String err) throws Exception {
				return err.matches("(?i:.*" + error + ".*)") ? true : false;
			}
		});
	}


}