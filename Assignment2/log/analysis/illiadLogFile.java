package log.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.net.PrintCommandListener;
import org.apache.hadoop.fs.shell.Count;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.MapPartitionsRDD;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class illiadLogFile {

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {

		

		String logFileIliad = "/home/karunsh/workspace/RecomSystem/Files/iliad/part-0000[0-4]*";
		String logFileOdyssey = "/home/karunsh/workspace/RecomSystem/Files/odyssey/part-0000[0-4]*";
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");
		// SparkSession spark =
		// SparkSession.builder().appName("JavaSparkSQL").master("local[*]").getOrCreate();

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);
// Print Question 1
		System.out.println("Question 1 Line Counts\n + Iliad :" + logRDDIllad.count() + "\n" + "+ Odyssey :"
				+ logRDDOdyssey.count());
		

		JavaRDD<String> sessionCount_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "achille");
		JavaRDD<String> sessionCount_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "achille");
		
// Print Question 2		
		System.out.println("Question 2 Sessions of user 'achille'\n + Iliad :" + sessionCount_Iliad.count() + "\n"
				+ "+ Odyssey :" + sessionCount_oddyssey.count());
		
		JavaRDD<String> sessionDetail_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "user");
		JavaRDD<String> sessionDetail_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "user");
		
		
		
		

	    //	JavaRDD<String> sessionDetail_oddessey_split =sessionDetail_oddessey.flatMap(line -> Arrays.asList(line.substring(beginIndex, endIndex)).iterator());

		
		/*JavaRDD<String> uniqueUser = sessionDetail_Iliad.filter(new Function<String, Boolean>() {

			
			String userName = null;
			int indexOfUser ;
			List<String> uniqUsers = new ArrayList<>();
			Set<String> hs = new HashSet<>();
			public Boolean call(String uUser) throws Exception {
				// TODO Auto-generated method stub
				uUser.indexOf("user");
				int endIndex = uUser.indexOf(".");
				userName = uUser.substring(indexOfUser + 5, endIndex);
				uniqUsers.add(userName);
				hs.addAll(uniqUsers);
				uniqUsers.clear();
				uniqUsers.addAll(hs);
				
				return uniqUsers.contains(userName)? true:false;
			}
		});
		List<String> sessionUserIliad1 = uniqueUser.collect();
		System.out.println("test"+uniqueUser.take(1));*/
//Print Question 3		
		List<String> sessionUserIliad = sessionUsers(sessionDetail_Iliad);
		List<String> sessionUserOddessey = sessionUsers(sessionDetail_oddyssey);
		System.out.println("Question 3 unique user names \n + Iliad :" + sessionUserIliad + "\n"
				+ "+ Odyssey :" + sessionUserOddessey);

		/*for (int i = 0; i < sessionUserIliad.size(); i++) {

			System.out.println(i + ")" + sessionUserIliad.get(i));
		}*/
		JavaRDD<String> errorRDDIllad = getErrorCount(logRDDIllad,"error");
		JavaRDD<String> errorRDDOdyssey = getErrorCount(logRDDOdyssey,"error");
		
//Print Question 5	
		System.out.println("Question 5 number of errors \n + Iliad :" + errorRDDIllad.count() + "\n"
				+ "+ Odyssey :" + errorRDDOdyssey.count());
		
		
//Prnt question 7
		
		JavaRDD<String> rddUserIliad = sc.parallelize(sessionUserIliad); 
		JavaRDD<String> rddUserOddyssey = sc.parallelize(sessionUserOddessey); 
		
		JavaRDD<String> rddUserCombine = rddUserIliad.intersection(rddUserOddyssey);
		
	
		 
		System.out.println("Q7: users who started a session on both hosts \n + "+rddUserCombine.collect());
		
		
		
		
		
		

	}

	/**
	 * @param logRDDIllad
	 * @return
	 */
	public static JavaRDD<String> getErrorCount(JavaRDD<String> errorLogRDDFile, String error) {
		return errorLogRDDFile.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String err) throws Exception {			
				return err.matches("(?i:.*"+error+".*)")? true:false;
			}
		});
	}

	/**
	 * @param sessionDetail_oddessey
	 * @return
	 */
	public static List<String> sessionUsers(JavaRDD<String> sessionDetailRDD) {
		int indexOfUser;
		int endIndex;
		int count=0;
		String userName = null;
		List<String> uniqUsers = new ArrayList<>();
		Set<String> hs = new HashSet<>();
		List<String> detail = sessionDetailRDD.collect();
		for (String s : detail) {
			indexOfUser = (s.indexOf("user"));
			endIndex = s.indexOf(".");
			userName = s.substring(indexOfUser + 5, endIndex);

			uniqUsers.add(userName);
			/*if(s.contains("gaia"))
			{
				count++;
			}*/

		}
		//System.out.println("count"+count);

		hs.addAll(uniqUsers);
		uniqUsers.clear();
		uniqUsers.addAll(hs);
		return uniqUsers;
	}

	/**
	 * @param logRDDIllad
	 * @return
	 */
	public static JavaRDD<String> sessionDetaililliad(JavaRDD<String> logRDDIllad, String Session, String user) {
		JavaRDD<String> sessionCount = logRDDIllad.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String sessionC) throws Exception {
				// TODO Auto-generated method stub
				return (sessionC.contains(Session)) ? true : false;
			}
		});
		JavaRDD<String> sessionCount1 = sessionCount.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String sessionC) throws Exception {
				// TODO Auto-generated method stub
				return (sessionC.contains(user)) ? true : false;
			}
		});
		return sessionCount1;
	}
	

}
