package Log.LogAnalysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class q8Main {
	 
	

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
		

		JavaRDD<String> sessionDetail_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "user");
		JavaRDD<String> sessionDetail_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "user");
		JavaRDD<String> rddUserIliad = getSeesionUser(sessionDetail_Iliad).distinct();
		JavaRDD<String> rddUserOddyssey = getSeesionUser(sessionDetail_oddyssey).distinct();
		//get session user intersection
		JavaRDD<String> rddUserCommon = rddUserIliad.intersection(rddUserOddyssey);
		
		
		//get system user intersection
		JavaRDD<String> rddSysUserIliad = getSysUser(sessionDetail_Iliad);
		JavaRDD<String> rddSysUserodyssey = getSysUser(sessionDetail_oddyssey);
		JavaRDD<String> rddSysUserodyssey_local =rddSysUserodyssey.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				// TODO Auto-generated method stub
				return s.contains("localhost");
			}
		});
		JavaRDD<String> rddSysUserIliad_local =rddSysUserIliad.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String s) throws Exception {
				// TODO Auto-generated method stub
				return s.contains("localhost");
			}
		});
		
		rddSysUserIliad = rddSysUserIliad.subtract(rddSysUserIliad_local);
		rddSysUserodyssey = rddSysUserodyssey.subtract(rddSysUserodyssey_local);
		
		JavaRDD<String> rddSysUserunion = rddSysUserIliad.union(rddSysUserodyssey);
		//JavaRDD<String> rddSyssessionUserunion = rddSysUserunion.union(rddUserCommon);
		JavaPairRDD<String, String> rddSyssessionUserunionpair_INS= rddUserCommon.cartesian(rddSysUserunion);	
		JavaPairRDD<String, String> rddPairSysUserSessionUserPair_Iliad = getSysUserSessionUserPair(sessionDetail_Iliad);
		JavaPairRDD<String, String> rddPairSysUserSessionUserPair_Odyssey = getSysUserSessionUserPair(sessionDetail_oddyssey);	
		
		JavaPairRDD<String, String> rddPairSysUserSessionUserPair_Odyssey_localhost = getLocalhost(
				rddPairSysUserSessionUserPair_Odyssey);
		JavaPairRDD<String, String> rddPairSysUserSessionUserPair_Iliad_localhost = getLocalhost(
				rddPairSysUserSessionUserPair_Iliad);
		rddPairSysUserSessionUserPair_Odyssey = rddPairSysUserSessionUserPair_Odyssey.subtract(rddPairSysUserSessionUserPair_Odyssey_localhost);
		rddPairSysUserSessionUserPair_Iliad = rddPairSysUserSessionUserPair_Iliad.subtract(rddPairSysUserSessionUserPair_Iliad_localhost);
		
		JavaPairRDD<String, String>  rddPairSysUserSessionUserPair_Union=  rddPairSysUserSessionUserPair_Odyssey.union(rddPairSysUserSessionUserPair_Iliad);
		JavaPairRDD<String, String> rddeachhostEachUserPair = rddPairSysUserSessionUserPair_Union.subtract(rddSyssessionUserunionpair_INS);
		
		System.out.println("* Q8: users who started a session on exactly one host, with host name \n + :" + rddeachhostEachUserPair.collect());
		
	}

	


	/**
	 * @param rddPairSysUserSessionUserPair_Odyssey
	 * @return
	 */
	public static JavaPairRDD<String, String> getLocalhost(
			JavaPairRDD<String, String> rddPairSysUserSessionUserPair_Odyssey) {
		JavaPairRDD<String, String> rddPairSysUserSessionUserPair_Odyssey_localhost = rddPairSysUserSessionUserPair_Odyssey.filter(new Function<Tuple2<String,String>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, String> s) throws Exception {
				// TODO Auto-generated method stub
				return s._2.contains("localhost");
			}
		});
		return rddPairSysUserSessionUserPair_Odyssey_localhost;
	}


	/**
	 * @param sessionDetail_Iliad
	 * @return
	 */
	public static JavaPairRDD<String, String> getSysUserSessionUserPair(JavaRDD<String> sessionDetail_Iliad) {
		JavaPairRDD<String, String> rddPairSysUserSessionUserPairUnion = sessionDetail_Iliad.mapToPair(new PairFunction<String, String,String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				
				String sysUser = getSysUserSession(s);
				String sesseionUser = getSessionUser(s);
				
				return new Tuple2<String, String>(sesseionUser, sysUser);
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

			public String getSysUserSession(String s) {
				int endIndex = 0;
				String sysUser = null;
				endIndex = s.indexOf(" ", 16);
				sysUser = s.substring(16, endIndex).trim();
				return sysUser;
			}
		}).distinct();
		return rddPairSysUserSessionUserPairUnion;
	}

	///
	public static JavaRDD<String> getSysUser(JavaRDD<String> sessionDetail_Iliad) {
		JavaRDD<String> rddUserIliad = sessionDetail_Iliad.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {

				String sysUser = getSysUserSession(s);
				return (Iterator<String>) Arrays.asList(sysUser).iterator();
			}

			public String getSysUserSession(String s) {
				int endIndex = 0;
				String sysUser = null;
				endIndex = s.indexOf(" ", 16);
				sysUser = s.substring(16, endIndex).trim();
				return sysUser;
			
			}
		}).distinct();
		return rddUserIliad;
	}
	
	
	public static JavaRDD<String> getSeesionUser(JavaRDD<String> sessionDetail_Iliad) {
		JavaRDD<String> rddUserIliad = sessionDetail_Iliad.flatMap(new FlatMapFunction<String, String>() {
			@Override
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
	
	

	public static JavaRDD<String> sessionDetaililliad(JavaRDD<String> logRDDIllad, final String Session, final String user) {
		JavaRDD<String> sessionCount = logRDDIllad.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
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

			@Override
			public Boolean call(String sessionC) throws Exception {
				// TODO Auto-generated method stub
				return (sessionC.contains(user)) ? true : false;
			}
		});
		return sessionCount1;
	}
	
}