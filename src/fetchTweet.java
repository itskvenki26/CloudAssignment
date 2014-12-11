import java.io.File;
import java.io.PrintWriter;
import java.util.*;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.sql.*;


public class fetchTweet {
	public static int j = 0;

	public static JSONObject maintweets(String input) throws Exception 
	{
		JSONObject obj = new JSONObject();
		

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey("cD0uPF9gu1asdGdKYynYNdm2p");
		cb.setOAuthConsumerSecret("W3vp78Hkx9QyK5rdyuLrRz47zSHMLBLkegjHE3TY5VsB7Q4qV2");
		cb.setOAuthAccessToken("827487456-n5XBBQF43J1vOXDuRMweTezdso3hLbNeOCAsltzp");
		cb.setOAuthAccessTokenSecret("yc8tMaRwfMmWhiZ17j5cONQ7eQOga2eiNCm0AskuCtlmM");

		PrintWriter tweetWriter = null;

		Twitter twitter = new TwitterFactory(cb.build()).getInstance();
		Query query = new Query(input);
		int numberOfTweets = 5120;
		long lastID = Long.MAX_VALUE;
		ArrayList<Status> tweets = new ArrayList<Status>();


		while (tweets.size () < numberOfTweets) {
			if (numberOfTweets - tweets.size() > 100)
				query.setCount(100);
			else 
				query.setCount(numberOfTweets - tweets.size());
			try 
			{ 
				QueryResult result = twitter.search(query);
				tweets.addAll(result.getTweets());
				System.out.println("Gathered " + tweets.size() + " tweets");

				//storing the tweets in the current page to newhappy.txt i.e NOSQL
				tweetWriter = new PrintWriter(new File("newhappy.txt"));

				for (Status t: tweets) 
					if(t.getId() < lastID) lastID = t.getId();

			}

			catch (TwitterException te) {
				System.out.println("Couldn't connect: " + te);
			}; 
			query.setMaxId(lastID-1);
		}

		//LangLat(tweets);
		Connection c = null;
		Statement stmt = null;

		try
		{
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:tweets.db");
			c.setAutoCommit(false);
			System.out.println("Opened database successfully");
			stmt = c.createStatement();
			String sql = "CREATE TABLE if not exists tweets "
					+ "( twit_ID TEXT, "
					+ "User_Name TEXT NOT NULL, "
					+ "Latitude INT NOT NULL, "
					+ "Longitude INT NOT NULL, "
					+  "Date_time TEXT )";
			stmt.executeUpdate(sql);

			String sql_delete = "DELETE from tweets;";
			stmt.executeUpdate(sql_delete);
			int j=0;
			for (int i = 0; i < tweets.size(); i++) {

				Status t = (Status) tweets.get(i);

				System.out.println("In for Loop");

				GeoLocation loc = t.getGeoLocation();
				String user = t.getUser().getScreenName().toString();
				String msg = t.getText().trim().toString();
				long twitID = t.getId();
				String date = t.getCreatedAt().toString();

				String split[] = msg.split(":");

				String time = "";
				if (loc != null) {
					
					Double lat = t.getGeoLocation().getLatitude();
					Double lon = t.getGeoLocation().getLongitude();
					obj.put(lat+"", lon);

					j++;
					System.out.println(i + " located at " + lat + ", " + lon);
					String sql1 = "INSERT INTO tweets (twit_ID,User_Name,Latitude,Longitude,Date_time) "
							+ "VALUES ('"
							+ String.valueOf(twitID)
							+ "', '"
							+ user + "', '"+lat+"', '" +lon+"', '" + date + "' );";

					stmt.executeUpdate(sql1);
				}
				else
					System.out.println(i);
			}
		}
		catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		//List<double[]> rowList = new ArrayList<double[]>();

		ResultSet rs = stmt.executeQuery( "SELECT * FROM tweets;" );
		while ( rs.next() ) {
			String id = rs.getString("twit_ID");
			String  name = rs.getString("User_Name");
			double  latitude = rs.getDouble("Latitude");
			double  longitude = rs.getDouble("Longitude");
			//String  msg  = rs.getString("message");
			String  date = rs.getString("Date_time");
			System.out.println( "twitID = " + id );
			System.out.println( "NAME = " + name );
			//System.out.println( "Message = " + msg );
			System.out.println( "date = " + date );
			System.out.println( latitude );
			System.out.println( longitude );
			System.out.println();
		}

		stmt.close();
		c.commit();
		c.close();



		//	  	System.out.println(result[0].length);
		//	  	System.out.println(result[0][1]);
		//	  	System.out.println(result[1][0]);
		//	  	System.out.println(result[1][1]);
		//	  	System.out.println(result[1][2]);
		System.out.println(obj);
		return obj;
		//-23.31726564, -46.58694658

	}
}
//	public static double[][] LangLat(ArrayList<Status> tw)
//	
//	{
//		
//		//double result [][] = new double[][]{lati, longi};
//		//return result;
//	
//	}