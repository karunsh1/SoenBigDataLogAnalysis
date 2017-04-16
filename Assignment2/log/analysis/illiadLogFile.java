package log.analysis;

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

public class illiadLogFile {

	@SuppressWarnings({ "resource", "unchecked", "rawtypes" })
	public static void main(String[] args) {

		String logFileIliad = "/home/karunsh/workspace/RecomSystem/Files/iliad/part-0000[0-4]*";
		String logFileOdyssey = "/home/karunsh/workspace/RecomSystem/Files/odyssey/part-0000[0-4]*";
		SparkConf conf = new SparkConf().setAppName("Log Analysis").setMaster("local[*]");

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

		
		// Print Question 3		
		
		JavaRDD<String> rddUserIliad = getSeesionUser(sessionDetail_Iliad).distinct();
		JavaRDD<String> rddUserOddyssey = getSeesionUser(sessionDetail_oddyssey).distinct();		
		
		
		System.out.println("Question 3 unique user names \n + Iliad :" + rddUserIliad.collect() + "\n" + "+ Odyssey :"
				+ rddUserOddyssey.collect());
		
		
		//Q-4
		JavaPairRDD<String, Integer> rddeUserSessionCount_Iliad = getSessionCountwithUser(sessionDetail_Iliad);
		JavaPairRDD<String, Integer> rddeUserSessionCount_Oddyssey = getSessionCountwithUser(sessionDetail_oddyssey);
		
		
		System.out.println("* Q4: sessions per user  \n + Iliad :"+rddeUserSessionCount_Iliad.collect() + "\n + Odyssey :"
				+ rddeUserSessionCount_Oddyssey.collect());

		JavaRDD<String> errorRDDIllad = getErrorCount(logRDDIllad, "error");
		JavaRDD<String> errorRDDOdyssey = getErrorCount(logRDDOdyssey, "error");

		// Print Question 5
		System.out.println("Question 5 number of errors \n + Iliad :" + errorRDDIllad.count() + "\n + Odyssey :"
				+ errorRDDOdyssey.count());

		// Print Question 6
		List<Tuple2<Integer, String>> most5FrequentMessages_iliad = getTop5Errormessages(sc, errorRDDIllad);
		List<Tuple2<Integer, String>> most5FrequentMessages_Odyssey = getTop5Errormessages(sc, errorRDDOdyssey);
		System.out.println("5 most frequent error messages \n +  Iliad :" + most5FrequentMessages_iliad
				+ "\n + Odyssey :" + most5FrequentMessages_Odyssey);

		// Print question 7

		JavaRDD<String> rddUserCombine = rddUserIliad.intersection(rddUserOddyssey);
		System.out.println("Q7: users who started a session on both hosts \n + " + rddUserCombine.collect());
		// R question 8

		//JavaRDD< Tuple2<String,String>> UserSyspair 
		JavaRDD<String> UserSyspair = sessionDetail_Iliad.flatMap(new FlatMapFunction<String, String>() {
		      @Override
		      public Iterator<String> call(String s) {   	  
		    	  String sysSesionUser = null;
		    	  String sysUser = getSysUserSession(s);
		    	  String sesseionUser = getSessionUser(s);
		    	  
		    	  sysSesionUser = "("+sesseionUser+","+sysUser+")";
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
		      		int endIndex=0;
		      		int indexofuser = 0;
					String sysUser = null;
					String temp = null;
					Pattern pattern = Pattern.compile("^(\\S+) \\d{2} (\\S+) ");

					Matcher matcher = pattern.matcher(s);
					while (matcher.find()) {

						temp = matcher.group().trim();
						indexofuser = matcher.end();
					}
					
					 endIndex = s.indexOf(" ",indexofuser);
					 sysUser = s.substring(indexofuser,endIndex).trim();
					return sysUser;
				}
		    }).distinct();				

		System.out.println(UserSyspair.collect());

		// Print question 9

		int indexofuser = 0;
		String sessionUser = null;
		String temp = null;
		List<String> errorList = new ArrayList<>();
		List<String> temprddlist = sessionDetail_Iliad.collect();
		for (String s : temprddlist) {
			Pattern pattern = Pattern
					.compile("^(\\S+) \\d{2} (\\S+) ");

			Matcher matcher = pattern.matcher(s);
			while (matcher.find()) {

				temp = matcher.group();
				indexofuser = matcher.end();
				// Matcher matcherSysUser = patternSysUser.matcher(temp);

			}			
		    int index1 = s.indexOf(" ",indexofuser);
			sessionUser = s.substring(indexofuser,index1).trim();
			//System.out.println("index :" + indexofuser + "value :" + sessionUser);
		}
		
		
		//System.out.println("index :" + indexofuser + "value :" + sessionUser);

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
	public static JavaPairRDD<String, Integer> getSessionCountwithUser(
			JavaRDD<String>  sessionDetail) {
		JavaRDD<String> rddSessionUser_Iliad = getSeesionUser(sessionDetail);
		JavaPairRDD<String, Integer> rddeUserSessionmap = rddSessionUser_Iliad
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

	public static JavaRDD<String> getErrorCount(JavaRDD<String> errorLogRDDFile, String error) {
		return errorLogRDDFile.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String err) throws Exception {
				return err.matches("(?i:.*" + error + ".*)") ? true : false;
			}
		});
	}

	/*public static List<String> getUsers(JavaRDD<String> sessionDetailRDD) {
		int indexOfUser;
		int endIndex;
		String userName = null;
		List<String> uniqUsers = new ArrayList<>();
		// Set<String> hs = new HashSet<>();
		List<String> detail = sessionDetailRDD.collect();
		for (String s : detail) {
			indexOfUser = (s.indexOf("user"));
			endIndex = s.indexOf(".");
			userName = s.substring(indexOfUser + 5, endIndex);

			uniqUsers.add(userName);

		}
		// hs.addAll(uniqUsers);
		// uniqUsers.clear();
		// uniqUsers.addAll(hs);
		return uniqUsers;
	}*/

	/**
	 * @param logRDDIllad
	 * @return
	 */
	public static JavaRDD<String> sessionDetaililliad(JavaRDD<String> logRDDIllad, String Session, String user) {
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