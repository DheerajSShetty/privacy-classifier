package edu.ncsu.csc.privacy.db;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONObject;


public class NewsDbMgr {

	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

	  private Connection mConn = null;

	  private PreparedStatement mPrepdStmt = null;

	  private int mCounter = 0;

	  public boolean init(String dbUrl, String user, String pass) {
	    try {
	      Class.forName(MYSQL_DRIVER).newInstance();
	      mConn = DriverManager.getConnection(dbUrl, user, pass);
	      
	      //ScriptRunner runner = new ScriptRunner(mConn, true, true);
	      //runner.runScript(new BufferedReader(new FileReader("sql/create_schema_basic.sql")));

	      mPrepdStmt = mConn
	          .prepareStatement("insert into tweet_status_object (status_object) values (?)");
	    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
	        | SQLException e) {
	      System.err.println("There was an error initializing the database");
	      e.printStackTrace();
	      return false;
	    }

	    return true;
	  }

	  public void close() {
	    try {
	      if (mPrepdStmt != null && !mPrepdStmt.isClosed()) {
	        mPrepdStmt.executeBatch();
	        mPrepdStmt.close();
	      }

	      if (mConn != null && !mConn.isClosed()) {
	        mConn.close();
	      }
	    } catch (SQLException e) {
	      System.err.println("There was an error closing DB connection.");
	      e.printStackTrace();
	    }
	  }

	  public boolean insertObject(JSONObject status) {
	    try {
	      mPrepdStmt.setObject(1, status);
	      mPrepdStmt.addBatch();
	      if (mCounter++ % 100 == 0) {
	        mPrepdStmt.executeBatch();
	      }
	      System.out.println("Number of tweets collected so far: " + mCounter);
	    } catch (SQLException e) {
	      System.err.println("Error inserting to the database");
	      return false;
	    }

	    return true;
	  }

	  public static boolean unmarshalStatusObject(String srcDbUrl, String destDbUrl) {
	    try {
	      Class.forName(MYSQL_DRIVER);
	    } catch (ClassNotFoundException e) {
	      System.err.println("Driver not found");
	      e.printStackTrace();
	      return false;
	    }

	    try (Connection srcConn = DriverManager.getConnection(srcDbUrl);
	        Connection destConn = DriverManager.getConnection(destDbUrl);) {

	      destConn.setAutoCommit(false);

	      // Create tables, if they don't exist.
	      //ScriptRunner runner = new ScriptRunner(destConn, false, true);
	      //runner.runScript(new BufferedReader(new FileReader("sql/create_schema_advanced.sql")));

	      long maxId = getMaxStatusDetailsId(destConn);
	      long fetchSize = 10000;
	      long resultsetSize = 0;

	      do {
	        try (Statement srcStmt = srcConn.createStatement();

	            PreparedStatement destInsertObjPrepdStmt = destConn
	                .prepareStatement("insert into tweet_status_object (" +
	                    "id, " +
	                    "status_object) " +
	                    "values (?, ?)");

	            PreparedStatement destInsertDetailsPrepdStmt = destConn.prepareStatement(
	                "insert ignore into tweet_status_details (" +
	                    "tweet_id, " +
	                    "tweet_object_id, " +
	                    "creation_time, " +
	                    "text, " +
	                    "geo_location, " +
	                    "source, " +
	                    "user_id, " +
	                    "is_truncated, " +
	                    "in_reply_to_status_id, " +
	                    "in_reply_to_user_id, " +
	                    "in_reply_to_user_screen_name, " +
	                    "is_favorited, " +
	                    "retweet_count, " +
	                    "is_retweeted_by_me, " +
	                    "current_user_retweet_id, " +
	                    "is_possibly_sensitive)"
	                    + "values (?, ?, ?, ?, GeomFromText(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

	            PreparedStatement destInsertUserPrepdStmt = destConn
	                .prepareStatement("insert ignore into tweet_users (" +
	                    "tweet_id, " +
	                    "user_id, " +
	                    "user_name, " +
	                    "screen_name, " +
	                    "location, " +
	                    "description, " +
	                    "is_contributor_enabled, " +
	                    "url, " +
	                    "is_protected, " +
	                    "follower_count, " +
	                    "friends_count, " +
	                    "creation_time, " +
	                    "favorites_count, " +
	                    "timezone, " +
	                    "utc_offset, " +
	                    "lang, " +
	                    "status_count, " +
	                    "is_geo_enabled, " +
	                    "is_verified, " +
	                    "is_translator, " +
	                    "listed_count, " +
	                    "is_follow_request_sent) " +
	                    "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

	            PreparedStatement destInsertContribsPrepdStmt = destConn
	                .prepareStatement("insert into tweet_contributors (" +
	                    "tweet_id, " +
	                    "contributor_id) " +
	                    "values (?, ?)");) {

	          resultsetSize = 0;
	          ResultSet rs = srcStmt.executeQuery("select * from tweet_status_object where id > " + maxId
	              + " limit " + fetchSize);

	          while (rs.next()) {
	            long id = rs.getLong("id");
	            byte[] buf = rs.getBytes("status_object");
	            if (buf != null) {
	              ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
	              JSONObject status = (JSONObject) objectIn.readObject();
	              unmarshalStatusObject(id, status, destInsertObjPrepdStmt, destInsertDetailsPrepdStmt,
	                  destInsertUserPrepdStmt, destInsertContribsPrepdStmt);
	            }
	            resultsetSize++;
	          }

	          rs.close();
	          destInsertObjPrepdStmt.executeBatch();
	          destInsertDetailsPrepdStmt.executeBatch();
	          destInsertUserPrepdStmt.executeBatch();
	          destInsertContribsPrepdStmt.executeBatch();
	          
	          maxId += resultsetSize;
	        } finally {
	          destConn.commit();
	          System.out.println("Commited rows so far: " + maxId);
	        }
	      } while (resultsetSize >= fetchSize);

	    } catch (SQLException | IOException | ClassNotFoundException e1) {
	      System.err.println("Error setting up the destination database");
	      e1.printStackTrace();
	      return false;
	    }

	    return true;
	  }

	  private static void unmarshalStatusObject(long id, JSONObject status,
	      PreparedStatement destInsertObjPrepdStmt, PreparedStatement destInsertDetailsPrepdStmt,
	      PreparedStatement destInsertUserPrepdStmt, PreparedStatement destInsertContribsPrepdStmt)
	      throws SQLException {
	    /*
	     * // Insert the object as it is. 
	     * destInsertObjPrepdStmt.setLong(1, id);
	     * destInsertObjPrepdStmt.setObject(2, status);
	     * destInsertObjPrepdStmt.addBatch();
	     */

	    // Strip the object and insert details.
	    /*destInsertDetailsPrepdStmt.setLong(1, status.getId());
	    destInsertDetailsPrepdStmt.setLong(2, id);
	    destInsertDetailsPrepdStmt.setTimestamp(3, new java.sql.Timestamp(status.getCreatedAt()
	        .getTime()));
	    destInsertDetailsPrepdStmt.setString(4, status.getText());
	    if (status.getGeoLocation() != null) {
	      destInsertDetailsPrepdStmt.setString(5, "POINT(" + status.getGeoLocation().getLongitude()
	          + " " + status.getGeoLocation().getLatitude() + ")");
	    } else {
	      destInsertDetailsPrepdStmt.setString(5, null);
	    }
	    destInsertDetailsPrepdStmt.setString(6, status.getSource());
	    destInsertDetailsPrepdStmt.setLong(7, status.getUser().getId());
	    destInsertDetailsPrepdStmt.setBoolean(8, status.isTruncated());
	    destInsertDetailsPrepdStmt.setLong(9, status.getInReplyToStatusId());
	    destInsertDetailsPrepdStmt.setLong(10, status.getInReplyToUserId());
	    destInsertDetailsPrepdStmt.setString(11, status.getInReplyToScreenName());
	    destInsertDetailsPrepdStmt.setBoolean(12, status.isFavorited());
	    destInsertDetailsPrepdStmt.setLong(13, status.getRetweetCount());
	    destInsertDetailsPrepdStmt.setBoolean(14, status.isRetweetedByMe());
	    destInsertDetailsPrepdStmt.setLong(15, status.getCurrentUserRetweetId());
	    destInsertDetailsPrepdStmt.setBoolean(16, status.isPossiblySensitive());
	    destInsertDetailsPrepdStmt.addBatch();
	    // System.out.println(destInsertDetailsPrepdStmt.toString());

	    // Insert User details
	    destInsertUserPrepdStmt.setLong(1, status.getId());
	    destInsertUserPrepdStmt.setLong(2, status.getUser().getId());
	    destInsertUserPrepdStmt.setString(3, status.getUser().getName());
	    destInsertUserPrepdStmt.setString(4, status.getUser().getScreenName());
	    destInsertUserPrepdStmt.setString(5, status.getUser().getLocation());
	    destInsertUserPrepdStmt.setString(6, status.getUser().getDescription());
	    destInsertUserPrepdStmt.setBoolean(7, status.getUser().isContributorsEnabled());
	    destInsertUserPrepdStmt.setString(8, status.getUser().getURL());
	    destInsertUserPrepdStmt.setBoolean(9, status.getUser().isProtected());
	    destInsertUserPrepdStmt.setInt(10, status.getUser().getFollowersCount());
	    destInsertUserPrepdStmt.setInt(11, status.getUser().getFriendsCount());
	    destInsertUserPrepdStmt.setTimestamp(12, new java.sql.Timestamp(status.getUser().getCreatedAt()
	        .getTime()));
	    destInsertUserPrepdStmt.setLong(13, status.getUser().getFavouritesCount());
	    destInsertUserPrepdStmt.setString(14, status.getUser().getTimeZone());
	    destInsertUserPrepdStmt.setInt(15, status.getUser().getUtcOffset());
	    destInsertUserPrepdStmt.setString(16, status.getUser().getLang());
	    destInsertUserPrepdStmt.setInt(17, status.getUser().getStatusesCount());
	    destInsertUserPrepdStmt.setBoolean(18, status.getUser().isGeoEnabled());
	    destInsertUserPrepdStmt.setBoolean(19, status.getUser().isVerified());
	    destInsertUserPrepdStmt.setBoolean(20, status.getUser().isTranslator());
	    destInsertUserPrepdStmt.setInt(21, status.getUser().getListedCount());
	    destInsertUserPrepdStmt.setBoolean(22, status.getUser().isFollowRequestSent());
	    destInsertUserPrepdStmt.addBatch();

	    // Insert contributors multi-values field into a separate table.
	    long contribs[] = status.getContributors();
	    for (long contrib : contribs) {
	      destInsertContribsPrepdStmt.setLong(1, status.getId());
	      destInsertContribsPrepdStmt.setLong(1, contrib);
	      destInsertContribsPrepdStmt.addBatch();
	    }
*/	  }

	  private static long getMaxStatusDetailsId(Connection conn) throws SQLException {
	    long maxId = 0;
	    try (Statement stmt = conn.createStatement();
	        ResultSet rs = stmt.executeQuery("select max(tweet_object_id) from tweet_status_details");) {
	      while (rs.next()) {
	        maxId = rs.getLong("max(tweet_object_id)");
	      }
	    }

	    return maxId;
	  }

	  public static void main(String[] args) {
	    NewsDbMgr.unmarshalStatusObject(
	        "jdbc:mysql://localhost:3306/twitter_election2014?user=root&password=qwerty",
	        "jdbc:mysql://localhost:3306/twitter_election2014?user=root&password=qwerty");
	  }
	
}
