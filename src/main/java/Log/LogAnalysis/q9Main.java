package Log.LogAnalysis;

import java.io.File;
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
import org.apache.spark.sql.catalyst.expressions.Coalesce;

import scala.Tuple2;

public class q9Main {

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {

		
		String logFileIliad = args[0];
		String logFileOdyssey = args[1];

		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logRDDIllad = sc.textFile(logFileIliad);
		JavaRDD<String> logRDDOdyssey = sc.textFile(logFileOdyssey);
		JavaRDD<String> sessionDetail_Iliad = sessionDetaililliad(logRDDIllad, "Starting Session", "user");
		
		JavaRDD<String> sessionDetail_oddyssey = sessionDetaililliad(logRDDOdyssey, "Starting Session", "user");
		JavaRDD<String> logRDDIllad_starting = logRDDIllad.filter(s->s.contains("Started Session"));
		JavaRDD<String> logRDDOdyssey_starting = logRDDOdyssey.filter(s->s.contains("Started Session"));
		JavaRDD<String> logRDDIllad_startedUnion = logRDDIllad_starting.union(sessionDetail_Iliad);
		JavaRDD<String> logRDDOdysey_startedUnion = logRDDOdyssey_starting.union(sessionDetail_oddyssey);
		
		JavaRDD<String> logRDDIllad_startedWithoutSession = logRDDIllad.subtract(logRDDIllad_startedUnion);
		JavaRDD<String> logRDDOdyssey_startedWithoutSession = logRDDOdyssey.subtract(logRDDOdysey_startedUnion);
		
		JavaRDD<String> rddUserIliad = getSeesionUser(sessionDetail_Iliad).distinct();
		JavaRDD<String> rddUserOddyssey = getSeesionUser(sessionDetail_oddyssey).distinct();
		// sorted session user
		rddUserIliad = getSortStringValue(rddUserIliad);
		rddUserOddyssey = getSortStringValue(rddUserOddyssey);

		JavaPairRDD<String, String> pairIliadTobeUser = getPairSessionUserToBeUser(rddUserIliad);
		JavaPairRDD<String, String> pairOdysseyTobeUser = getPairSessionUserToBeUser(rddUserOddyssey);

		
		
		JavaRDD<String> rddIliadAnonymized = getAnonymizedFile(logRDDIllad_startedUnion,
				logRDDIllad_startedWithoutSession, pairIliadTobeUser).coalesce(4);
		
		JavaRDD<String> rddOdysseyAnonymized = getAnonymizedFile(logRDDOdysey_startedUnion,
				logRDDOdyssey_startedWithoutSession, pairOdysseyTobeUser).coalesce(4);

		String iliadAnonymized = System.getProperty("user.home")+"/iliad-anonymized-10";
		String odYsseyAnonymized = System.getProperty("user.home")+"/odyssey-anonymized-10";
		
		
		rddIliadAnonymized.coalesce(4, false).saveAsTextFile(iliadAnonymized);
		rddOdysseyAnonymized.saveAsTextFile(odYsseyAnonymized);
		
		

		
		System.out.println("value: "+rddIliadAnonymized.count()+"   "+ rddOdysseyAnonymized.count() +"main" +logRDDIllad.count() + "  "+logRDDOdyssey.count());
		System.out.println("Q9 - Anonymize the logs \n + iliad: \n.User name mapping:" + pairIliadTobeUser.collect() + "\n. Anonymized files: " +iliadAnonymized +"\n"
		+ "\n + odyssey:\n .User name mapping:"  + pairOdysseyTobeUser.collect()
		+ "\n. Anonymized files: " +odYsseyAnonymized);
		

	}

	/**
	 * @param logRDDIllad_startedUnion
	 * @param logRDDIllad_startedWithoutSession
	 * @param pairIliadTobeUser
	 * @return
	 */
	public static JavaRDD<String> getAnonymizedFile(JavaRDD<String> logRDDIllad_startedUnion,
			JavaRDD<String> logRDDIllad_startedWithoutSession, JavaPairRDD<String, String> pairIliadTobeUser) {
		final List<Tuple2<String, String>> listpairIliadTobeUser  = pairIliadTobeUser.collect();
		
		JavaRDD<String> rddIliadAnonymized = logRDDIllad_startedUnion .map(new Function<String,String>() {
			List<Tuple2<String, String>> listpairIliadTobeUsertemp = listpairIliadTobeUser ;
			
			String m = null;
			public String call(String s) throws Exception {
				
				for(Tuple2<String, String> v: listpairIliadTobeUsertemp)
				{
					if(getSessionUser(s).equals(v._1))
					{
						m= s.replace(getSessionUser(s), v._2);
						
					}
					
				}
				
				return m;
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
		rddIliadAnonymized = rddIliadAnonymized.union(logRDDIllad_startedWithoutSession);
		return rddIliadAnonymized;
	}

	/**
	 * @param rddUserIliad
	 */
	public static JavaPairRDD<String, String> getPairSessionUserToBeUser(JavaRDD<String> rddUserIliad) {
		int rddiliadcount = (int) rddUserIliad.count();

		List<String> toBeUser = new ArrayList<>();
		for (int i = 0; i <= rddiliadcount; i++) {
			String user = "user-" + i;
			toBeUser.add(user);
		}

		JavaPairRDD<String, String> pairSessionUserTobeUser = rddUserIliad
				.mapToPair(new PairFunction<String, String, String>() {
					int count = 0;

					@Override
					public Tuple2<String, String> call(String s) throws Exception {

						String user;

						Tuple2<String, String> pairUserTobeUser = null;
						/*
						 * List<String> toBeUser = new ArrayList<>(); for (int i
						 * = 0; i < rddcount; i++) {
						 */
						user = toBeUser.get(count);
						pairUserTobeUser = new Tuple2<String, String>(s, user);

						count++;

						return pairUserTobeUser;
					}
				});
		return pairSessionUserTobeUser;
	}

	public static JavaRDD<String> getSortStringValue(JavaRDD<String> rddUserIliad) {
		JavaRDD<String> rddUserIliadSorted = rddUserIliad.sortBy(new Function<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String value) throws Exception {
				return value;
			}
		}, true, 1);
		return rddUserIliadSorted;
	}

	

	/**
	 * @param sessionDetail_Iliad
	 * @return
	 */
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

	
	
	
	public static JavaRDD<String> sessionDetaililliad(JavaRDD<String> logRDDIllad, final String Session,
			final String user) {
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