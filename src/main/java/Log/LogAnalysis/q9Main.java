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
		JavaRDD<String> logRDDIllad_startedUnion = logRDDIllad_starting.union(sessionDetail_Iliad);
		JavaRDD<String> rddUserIliad = getSeesionUser(sessionDetail_Iliad).distinct();
		JavaRDD<String> rddUserOddyssey = getSeesionUser(sessionDetail_oddyssey).distinct();
		// sorted session user
		rddUserIliad = getSortStringValue(rddUserIliad);
		rddUserOddyssey = getSortStringValue(rddUserOddyssey);

		JavaPairRDD<String, String> pairIliadTobeUser = getPairSessionUserToBeUser(rddUserIliad);
		JavaPairRDD<String, String> pairOdysseyTobeUser = getPairSessionUserToBeUser(rddUserOddyssey);

		
		
		final List<Tuple2<String, String>> listpairIliadTobeUser  = pairIliadTobeUser.collect();
		
		JavaRDD<String> rddIliadAnonymized = logRDDIllad_startedUnion .flatMap(new FlatMapFunction<String, String>() {
			List<Tuple2<String, String>> listpairIliadTobeUsertemp = listpairIliadTobeUser ;			
			
			public Iterator<String> call(String s) throws Exception {
				// TODO Auto-generated method stub
				
				for(Tuple2<String, String> v: listpairIliadTobeUsertemp)
				{
					if(v._1 == getSessionUser(s) )
					{
						s= s.replace(getSessionUser(s), v._2);
						System.out.println( getSessionUser(s)+"   tuple1   "+v._1 );
						
						
					}
					else {
						//System.out.println( getSessionUser(s)+"   tuple1   "+v._1 );
					}
					//System.out.println("changestring    :   "+  s);
					break;
				}			
				
				return (Iterator<String>) Arrays.asList( s).iterator();
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
			
			


			/*@Override
			public Iterator<Tuple2<String, String>> call(String s) throws Exception {
				List<Tuple2<String, String>> listpairIliadTobeUsertemp = listpairIliadTobeUser ;
				for(Tuple2<String, String> v: listpairIliadTobeUsertemp)
				{
					if(s.matches(v._1))
					{
						s= s.replace(getSessionUser(s), v._2);
						
					}
					
				}
							
				
				return listpairIliadTobeUsertemp.iterator();
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
		});*/
		
		//System.out.println("value: "+rddIliadAnonymized.filter(s->s.contains("user-1")).take(10));
		System.out.println("Q9 - Anonymize the logs \n + iliad: \n.User name mapping:" + pairIliadTobeUser.collect() + "\n. Anonymized files: "+ rddIliadAnonymized.take(200)
		+ "\n + odyssey:\n .User name mapping:"  + pairOdysseyTobeUser.collect()
		+ "\n. Anonymized files: ");
		

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

	public static JavaRDD<String> getSysUserSessionUserPair(JavaRDD<String> sessionDetail_oddyssey) {
		JavaRDD<String> UserSyspair = sessionDetail_oddyssey.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				String sysSesionUser = null;
				String sysUser = getSysUserSession(s);
				String sesseionUser = getSessionUser(s);
				sysSesionUser = "(" + sesseionUser + "," + sysUser + ")";
				return (Iterator<String>) Arrays.asList(sysSesionUser).iterator();
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
		return UserSyspair;
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

	/**
	 * @param sc
	 * @param errorRDDIllad
	 * @return
	 */
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

	/*
	 * public static List<String> getUsers(JavaRDD<String> sessionDetailRDD) {
	 * int indexOfUser; int endIndex; String userName = null; List<String>
	 * uniqUsers = new ArrayList<>(); // Set<String> hs = new HashSet<>();
	 * List<String> detail = sessionDetailRDD.collect(); for (String s : detail)
	 * { indexOfUser = (s.indexOf("user")); endIndex = s.indexOf("."); userName
	 * = s.substring(indexOfUser + 5, endIndex);
	 * 
	 * uniqUsers.add(userName);
	 * 
	 * } // hs.addAll(uniqUsers); // uniqUsers.clear(); // uniqUsers.addAll(hs);
	 * return uniqUsers; }
	 */

	/**
	 * @param logRDDIllad
	 * @return
	 */
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