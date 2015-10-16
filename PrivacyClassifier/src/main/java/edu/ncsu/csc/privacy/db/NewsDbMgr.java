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
	          .prepareStatement("insert into news_objects (object) values (?)");
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

	  public boolean insertNewsObject(String status) {
	    try {
	      
	      mPrepdStmt.setObject(1, status);
	      mPrepdStmt.addBatch();
	      System.out.println("Inserting object. mCounter: " + mCounter);
	      if (mCounter++ % 100 == 0) {
	        mPrepdStmt.executeBatch();
	      }
	      System.out.println("Number of news articles collected so far: " + mCounter);
	    } catch (SQLException e) {
	      System.err.println("Error inserting to the database" + e.toString());
	      return false;
	    }

	    return true;
	  }

	  public static boolean unmarshalNewsObject(String srcDbUrl, String destDbUrl) {
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
	                .prepareStatement("insert into news_objects (" +
	                    "id, " +
	                    "object) " +
	                    "values (?, ?)");

	            PreparedStatement destInsertDetailsPrepdStmt = destConn.prepareStatement(
	                "insert ignore into news_details (" +
	                    "id, " +
	                    "news_object_id, " +
	                    "web_url, " +
	                    "snippet, " +
	                    "lead_paragraph, " +
	                    "abstract, " +
	                    "print_page, " +
	                    "blog, " +
	                    "source, " +
	                    "multimedia, " +
	                    "headline, " +
	                    "keywords, " +
	                    "pub_date, " +
	                    "document_type, " +
	                    "news_desk, " +
	                    "section_name)" + 
	                    "subsection_name, " +
	                    "byline, " +
	                    "type_of_material, " +
	                    "_id, " +
	                    "word_count, " +
	                    "slideshow_credits, "
	                    + "values (?, ?, ?, ?, GeomFromText(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");) {

	          resultsetSize = 0;
	          ResultSet rs = srcStmt.executeQuery("select * from news_objects where id > " + maxId
	              + " limit " + fetchSize);

	          while (rs.next()) {
	            long id = rs.getLong("id");
	            byte[] buf = rs.getBytes("status_object");
	            if (buf != null) {
	              ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
	              JSONObject status = (JSONObject) objectIn.readObject();
	              unmarshalNewsObject(id, status, destInsertObjPrepdStmt, destInsertDetailsPrepdStmt);
	            }
	            resultsetSize++;
	          }

	          rs.close();
	          destInsertObjPrepdStmt.executeBatch();
	          destInsertDetailsPrepdStmt.executeBatch();
	          
	          
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

	  private static void unmarshalNewsObject(long id, JSONObject status,
	      PreparedStatement destInsertObjPrepdStmt, PreparedStatement destInsertDetailsPrepdStmt)
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
	        ResultSet rs = stmt.executeQuery("select max(news_object_id) from news_details");) {
	      while (rs.next()) {
	        maxId = rs.getLong("max(news_object_id)");
	      }
	    }

	    return maxId;
	  }

	  public static void main(String[] args) {
	    NewsDbMgr.unmarshalNewsObject(
	        "jdbc:mysql://localhost:3306/nytimes_privacy?user=root&password=qwerty",
	        "jdbc:mysql://localhost:3306/nytimes_privacy?user=root&password=qwerty");
	  }
	
}
