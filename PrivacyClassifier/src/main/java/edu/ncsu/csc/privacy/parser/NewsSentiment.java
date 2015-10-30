package edu.ncsu.csc.privacy.parser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.json.JSONObject;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;

public class NewsSentiment {
	
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	
	static int pos=0;
	static int neg=0;
	static int neu=0;
	static int unknown_label=0;
	
	
	public static String sentimentString(int sentiment) {
	    switch(sentiment) {
	    case 0:
	      return "Very negative";
	    case 1:
	      return "Negative";
	    case 2:
	      return "Neutral";
	    case 3:
	      return "Positive";
	    case 4:
	      return "Very positive";
	    default:
	      return "Unknown sentiment label " + sentiment;
	    }
	  }
	
	public static void fetchNewsObject(String dbUrl) {
	    try {
	      Class.forName(MYSQL_DRIVER);
	    } catch (ClassNotFoundException e) {
	      System.err.println("Driver not found");
	      e.printStackTrace();
	    }

	    try (Connection conn = DriverManager.getConnection(dbUrl);) {

	      conn.setAutoCommit(false);

	      // Create tables, if they don't exist.
	      //ScriptRunner runner = new ScriptRunner(destConn, false, true);
	      //runner.runScript(new BufferedReader(new FileReader("sql/create_schema_advanced.sql")));

	      long fetchSize = 1000;
	      long resultsetSize = 0;
	      long maxId = 0;
	      
	      do {
	        try { 
	        	Statement srcStmt = conn.createStatement();     		
	          resultsetSize = 0;
	          ResultSet rs = srcStmt.executeQuery("select * from news_details where lead_paragraph != '' and id > " + maxId
	              + " limit " + fetchSize);

	          while (rs.next()) {
	            long id = rs.getLong("id");
	            System.out.println(id);
	            
	            String paragraph  = rs.getString("lead_paragraph");
  	          System.out.println(paragraph);
  	          
  	          //getSentiment(paragraph);
  	          
  	          
  	        switch(getSentiment(paragraph)) {
  		    case 0:
  		    case 1:
  		      neg++;
  		    case 2:
  		      neu++;
  		    case 3:
  		    case 4:
  		      pos++;
  		    default:
  		      unknown_label++;
  		    }
  	          
	          
	            resultsetSize++;
	          }

	          rs.close();
	          
	          
	          maxId += resultsetSize;
	        } finally {
	        }
	      } while (resultsetSize >= fetchSize);
	      
	      System.out.println("Positive: " + pos);
	      System.out.println("Negative: " + neg);
	      System.out.println("Neutral: " + neu);
	      System.out.println("Unknown: " + unknown_label);

	    } catch (SQLException e1) {
	      System.err.println("Error setting up the destination database");
	      e1.printStackTrace();
	    }
	  }
	
	public static void readCSV(String filepath) {
		//filepath = "/PrivacyClassifier/data/type_news+source_nytimes+rand_400.csv";
		BufferedReader br = null;
		String line = "";
		String csvSplitBy = ",";
		
		int sentiments[] = new int[410];

		try {
			
			br = new BufferedReader(new FileReader(filepath));
			FileWriter sentiment_writer = new FileWriter("data/type_news+source_nytimes+rand_400-leadpara-sentiments.csv");
			int index=0;
			while ((line = br.readLine()) != null) {

				
				// use comma as separator
				//String[] news_entry = line.split(csvSplitBy);
				String lead_paragraph = line;
				if(lead_paragraph != null && !lead_paragraph.isEmpty() && lead_paragraph.trim() != "")
				sentiments[index] = getSentiment(lead_paragraph);
				
				switch(sentiments[index]) {
	  		    case 0:
	  		    case 1:
	  		      neg++; break;
	  		    case 2:
	  		      neu++; break;
	  		    case 3:
	  		    case 4:
	  		      pos++; break;
	  		    default:
	  		      unknown_label++;
	  		    }
				
				
				sentiment_writer.append(sentiments[index]+"\n");
				index++;
				System.out.println(index + ". " + lead_paragraph);

			}
			
			sentiment_writer.flush();
			sentiment_writer.close();


		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		System.out.println("Positive: " + pos);
	    System.out.println("Negative: " + neg);
	    System.out.println("Neutral: " + neu);
	    System.out.println("Unknown: " + unknown_label);
	}
	
	public static int getSentiment(String text) {
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		
		//String text = IOUtils.slurpFileNoExceptions(filename);
		Annotation annotation = new Annotation(text);
		pipeline.annotate(annotation);
		
		int sentiment = 2;
		
		int agg_sentiment = 0;
		int sentence_count = 0;
		
		for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
			  Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
			  sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			  System.err.println(sentence);
			  System.err.println("  Predicted sentiment: " + sentimentString(sentiment));
			  agg_sentiment += sentiment;
			  sentence_count++;
			}
		if(sentence_count == 0) {
			return 9;
		}
		else return agg_sentiment/sentence_count;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	    String parserModel = null;
		String sentimentModel = null;

		String filename = null;

		//tweetSentiment();
		
		//fetchNewsObject("jdbc:mysql://localhost:3306/nytimes_privacy?user=privacy_user&password=qwerty");
		readCSV("data/type_news+source_nytimes+rand_400-leadpara.csv");
		//System.out.println("Working Directory = " +  System.getProperty("user.dir"));
		  
		/*Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		
		//String text = IOUtils.slurpFileNoExceptions(filename);
		Annotation annotation = new Annotation("I hope no one buys Apple.");
		pipeline.annotate(annotation);
		
		for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
			  Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
			  int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			  System.err.println(sentence);
			  System.err.println("  Predicted sentiment: " + sentimentString(sentiment));
			}*/
	}
}
