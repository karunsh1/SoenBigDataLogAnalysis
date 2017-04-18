
//Ques 5 : For each host, count the error messages, i.e., the lines that contain string “error”
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

public class q5Main {

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {

	
		String logFileIliad = args[0];
		String logFileOdyssey = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);

		JavaRDD<String> errorRDDIllad = getErrorCount(logRDDIllad, "error");
		JavaRDD<String> errorRDDOdyssey = getErrorCount(logRDDOdyssey, "error");

		// Print Question 5
		System.out.println("Question 5 number of errors \n + Iliad :" + errorRDDIllad.count() + "\n + Odyssey :"
				+ errorRDDOdyssey.count());

	}
        ///method to get error count
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
