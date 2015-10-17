package edu.ncsu.csc.privacy.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;

import org.joda.time.LocalDate;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.ncsu.csc.privacy.db.NewsDbMgr;
/*import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.io.*;
import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentPipeline;
import edu.stanford.nlp.trees.*;
import edu.stanford.nlp.util.*;
*/
public class JsonReader {
	
	private static final String MYSQL_DB_URL = "jdbc:mysql://localhost/nytimes_privacy";
	private static final String USER = "root";
	private static final String PASS = "qwerty"; 

	private static NewsDbMgr mDbMgr = null;

	
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

	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}
	
	public static JSONObject readJsonFromUrl(String url) throws IOException,
			JSONException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is,
					Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			is.close();
		}
	}
	
    private static void fetchNewsByDate(String q, String begin_date, String end_date)  throws IOException, JSONException {
    	
    	System.out.println("Begin date: " + begin_date + ", End date: " + end_date);
    	
		//q = "privacy";
		String api_key = "7c28695c87ed6c0d43af6281da81c97c:8:73041019"; // Register on http://developer.nytimes.com
		//begin_date = "20100101"; //YYYYMMDD
		//end_date = "20101231"; //YYYYMMDD
		String sort = "oldest";
		int page = 0;

		JSONObject json = readJsonFromUrl("http://api.nytimes.com/svc/search/v2/articlesearch.json?q="
				+ q
				+ "&page="
				+ page
				+ "&sort="
				+ sort
				+ "&api-key="
				+ api_key
				+ "&begin_date=" + begin_date + "&end_date=" + end_date);

		//System.out.println(json.toString());
		// System.out.println(json.get("id"));
		JSONObject response = json.getJSONObject("response");

		int hits = Integer.parseInt(response.getJSONObject("meta")
				.get("hits").toString());
		int page_count = hits/10;
		
		
		while (page <= page_count) {
			
			/*try {
			    Thread.sleep(1000);                 //1000 milliseconds is one second.
			} catch(InterruptedException ex) {
			    Thread.currentThread().interrupt();
			}*/
			
			//System.out.println("Page: " + page);
			
			// System.out.println(response.get("docs"));
			JSONArray docs = response.getJSONArray("docs");
			// System.out.println(docs.toString());

			/*String[] web_url = new String[docs.length()];
			String[] lead_para = new String[docs.length()];
			String[] headline = new String[docs.length()];
*/
			for (int i = 0; i < docs.length(); i++) {
				//web_url[i] = docs.getJSONObject(i).get("web_url").toString();
				//lead_para[i] = docs.getJSONObject(i).get("lead_paragraph").toString();
				//headline[i] = docs.getJSONObject(i).getJSONObject("headline").get("main").toString();
				 
				 //System.out.println(docs.getJSONObject(i).get("web_url").toString());
				 mDbMgr = new NewsDbMgr();
			        if (!mDbMgr.init(MYSQL_DB_URL, USER, PASS)) {
			          try {
			        	  
						throw new Exception("Error initializing the database");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			        }
			        
			        if(!mDbMgr.insertNewsObject(docs.getJSONObject(i).toString())) {
	                    throw new IllegalStateException("Failed to insert to the dabase.");
	                  }
	       
			        
			        mDbMgr.close();
				
			}

			
			page++;

			json = readJsonFromUrl("http://api.nytimes.com/svc/search/v2/articlesearch.json?q="
					+ q
					+ "&page="
					+ page
					+ "&sort="
					+ sort
					+ "&api-key="
					+ api_key
					+ "&begin_date="
					+ begin_date
					+ "&end_date="
					+ end_date);

		}
		
	}

	public static void main(String[] args) throws IOException, JSONException {
		
		/*Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		*/
		/*int pos = 0;
		int neg = 0;
		int vpos = 0;
		int vneg = 0;
		int neutral = 0;
		*/
		/*Annotation annotation = new Annotation("I hope no one buys Apple.");
		pipeline.annotate(annotation);
		
		for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
			  Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
			  int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			  System.err.println(sentence);
			  System.err.println("  Predicted sentiment: " + sentimentString(sentiment));
			}
		*/
		
		
		SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
		
		for (LocalDate date = new LocalDate(2010,01,01) ; date.isBefore(new LocalDate(2015,10,01)); date = date.plusMonths(2))
		{
		    String begin_date = dateformat.format(date.toDate()).toString();
		    String end_date = dateformat.format(date.plusMonths(2).minusDays(1).toDate()).toString();
		    
		    System.out.println(begin_date + " - " + end_date);
		    fetchNewsByDate("privacy", begin_date, end_date);
		}
		

		
	    /*fetchNewsByDate("privacy", "20100101", "20100331");	
	    fetchNewsByDate("privacy", "20100401", "20100630");
	    fetchNewsByDate("privacy", "20100701", "20100930");
	    fetchNewsByDate("privacy", "20101001", "20101231");
	    
	    fetchNewsByDate("privacy", "20110101", "20110331");	
	    fetchNewsByDate("privacy", "20110401", "20110630");
	    fetchNewsByDate("privacy", "20110701", "20110930");
	    fetchNewsByDate("privacy", "20111001", "20111231");
	    
	    fetchNewsByDate("privacy", "20120101", "20120331");	
	    fetchNewsByDate("privacy", "20120401", "20120630");
	    fetchNewsByDate("privacy", "20120701", "20120930");
	    fetchNewsByDate("privacy", "20121001", "20121231");
	    
	    fetchNewsByDate("privacy", "20130101", "20130331");	
	    fetchNewsByDate("privacy", "20130401", "20130630");
	    fetchNewsByDate("privacy", "20130701", "20130930");
	    fetchNewsByDate("privacy", "20131001", "20131231");
	    
	    fetchNewsByDate("privacy", "20140101", "20140331");	
	    fetchNewsByDate("privacy", "20140401", "20140630");
	    fetchNewsByDate("privacy", "20140701", "20140930");
	    fetchNewsByDate("privacy", "20141001", "20141231");
	    
	    fetchNewsByDate("privacy", "20150101", "20150331");	
	    fetchNewsByDate("privacy", "20150401", "20150630");
	    fetchNewsByDate("privacy", "20150701", "20150930");
	    //fetchNewsByDate("privacy", "20151001", "20151231");
	    */
	    
	}
	
	
}
