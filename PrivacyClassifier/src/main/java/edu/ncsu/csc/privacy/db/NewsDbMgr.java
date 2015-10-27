package edu.ncsu.csc.privacy.db;

import java.io.IOException;
import java.io.InputStream;
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
	      //System.out.println("Inserting object. mCounter: " + mCounter);
	      if (mCounter++ % 100 == 0) {
	        mPrepdStmt.executeBatch();
	      }
	      //System.out.println("Number of news articles collected so far: " + mCounter);
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
	      long fetchSize = 1000;
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
	                    "section_name, " + 
	                    "subsection_name, " +
	                    "byline, " +
	                    "type_of_material, " +
	                    "_id, " +
	                    "word_count, " +
	                    "slideshow_credits) "
	                    + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");) {

	          resultsetSize = 0;
	          ResultSet rs = srcStmt.executeQuery("select * from news_objects where id > " + maxId
	              + " limit " + fetchSize);

	          while (rs.next()) {
	            long id = rs.getLong("id");
	            System.out.println(id);
	            
	            InputStream is = rs.getBinaryStream(2);
	            int ch;
	            
	            String object_string = "";

	          //read bytes from ByteArrayInputStream using read method
	          while((ch = is.read()) != -1)
	          {
	             //System.out.print((char)ch);
	             object_string = object_string + (char)ch;
	          }
	          
	          JSONObject news = new JSONObject(object_string);
	          
	          //System.out.println(news.toString());
	          System.out.println(news.get("web_url").toString());
	          
	          unmarshalNewsObject(id, news, destInsertObjPrepdStmt, destInsertDetailsPrepdStmt);
	          
	            //byte[] buf = rs.getBytes("object");
	            //if (buf != null) {
	              //ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
	              //JSONObject status = (JSONObject) objectIn.readObject();
	              
	              //unmarshalNewsObject(id, news, destInsertObjPrepdStmt, destInsertDetailsPrepdStmt);
	            //}
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

	    } catch (SQLException | IOException e1) {
	      System.err.println("Error setting up the destination database");
	      e1.printStackTrace();
	      return false;
	    }

	    return true;
	  }

	  private static void unmarshalNewsObject(long id, JSONObject status,
	      PreparedStatement destInsertObjPrepdStmt, PreparedStatement destInsertDetailsPrepdStmt)
	      throws SQLException {
		  
	    // Strip the object and insert details.
	    destInsertDetailsPrepdStmt.setLong(1, id);
	    destInsertDetailsPrepdStmt.setLong(2, id);
	    destInsertDetailsPrepdStmt.setString(3, status.get("web_url").toString());
	    destInsertDetailsPrepdStmt.setString(4, status.get("snippet").toString());
	    destInsertDetailsPrepdStmt.setString(5, status.get("lead_paragraph").toString());
	    destInsertDetailsPrepdStmt.setString(6, status.get("abstract").toString());
	    destInsertDetailsPrepdStmt.setString(7, status.get("print_page").toString());
	    destInsertDetailsPrepdStmt.setString(8, status.get("blog").toString());
	    destInsertDetailsPrepdStmt.setString(9, status.get("source").toString());
	    destInsertDetailsPrepdStmt.setString(10, status.get("multimedia").toString());
	    destInsertDetailsPrepdStmt.setString(11, status.get("headline").toString());
	    destInsertDetailsPrepdStmt.setString(12, status.get("keywords").toString());
	    destInsertDetailsPrepdStmt.setString(13, status.get("pub_date").toString());
	    destInsertDetailsPrepdStmt.setString(14, status.get("document_type").toString());
	    destInsertDetailsPrepdStmt.setString(15, status.get("news_desk").toString());
	    destInsertDetailsPrepdStmt.setString(16, status.get("section_name").toString());
	    destInsertDetailsPrepdStmt.setString(17, status.get("subsection_name").toString());
	    destInsertDetailsPrepdStmt.setString(18, status.get("byline").toString());
	    destInsertDetailsPrepdStmt.setString(19, status.get("type_of_material").toString());
	    destInsertDetailsPrepdStmt.setString(20, status.get("_id").toString());
	    destInsertDetailsPrepdStmt.setString(21, status.get("word_count").toString());
	    destInsertDetailsPrepdStmt.setString(22, status.get("slideshow_credits").toString());
	    destInsertDetailsPrepdStmt.addBatch();
	    System.out.println(destInsertDetailsPrepdStmt.toString());

	    }

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
	        "jdbc:mysql://localhost:3306/nytimes_privacy?user=privacy_user&password=qwerty",
	        "jdbc:mysql://localhost:3306/nytimes_privacy?user=privacy_user&password=qwerty");
	  }
	
}
