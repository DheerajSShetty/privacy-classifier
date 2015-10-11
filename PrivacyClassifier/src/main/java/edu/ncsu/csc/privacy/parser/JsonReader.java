package edu.ncsu.csc.privacy.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.stanford.nlp.ling.CoreAnnotations;
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

public class JsonReader {
	
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

	public static void main(String[] args) throws IOException, JSONException {
		
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		
		int pos = 0;
		int neg = 0;
		int vpos = 0;
		int vneg = 0;
		int neutral = 0;
		
		//String text = IOUtils.slurpFileNoExceptions(filename);
		Annotation annotation = new Annotation("I hope no one buys Apple.");
		pipeline.annotate(annotation);
		
		for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
			  Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
			  int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
			  System.err.println(sentence);
			  System.err.println("  Predicted sentiment: " + sentimentString(sentiment));
			}
		
		
		String q = "privacy";
		String api_key = "7c28695c87ed6c0d43af6281da81c97c:8:73041019";
		String begin_date = "20100101";
		String end_date = "20150927";
		String sort = "newest";
		int page = 1;

		JSONObject json = readJsonFromUrl("http://api.nytimes.com/svc/search/v2/articlesearch.json?q="
				+ q
				+ "&page="
				+ page
				+ "&sort="
				+ sort
				+ "&api-key="
				+ api_key
				+ "&begin_date=" + begin_date + "&end_date=" + end_date);

		System.out.println(json.toString());
		// System.out.println(json.get("id"));
		JSONObject response = json.getJSONObject("response");

		int hits = Integer.parseInt(response.getJSONObject("meta")
				.get("hits").toString());
		int offset = Integer.parseInt(response.getJSONObject("meta")
				.get("offset").toString());

		while (page < 10) {
			
			System.out.println("Page: " + page);
			
			// System.out.println(response.get("docs"));
			JSONArray docs = response.getJSONArray("docs");
			// System.out.println(docs.toString());

			String[] web_url = new String[docs.length()];
			String[] lead_para = new String[docs.length()];
			String[] headline = new String[docs.length()];

			for (int i = 0; i < docs.length(); i++) {
				web_url[i] = docs.getJSONObject(i).get("web_url").toString();
				lead_para[i] = docs.getJSONObject(i).get("lead_paragraph")
						.toString();
				headline[i] = docs.getJSONObject(i).getJSONObject("headline")
						.get("main").toString();
			}

			for (int i = 0; i < lead_para.length; i++) {
				//System.out.println(web_url[i]);
				
				annotation = new Annotation(lead_para[i]);
				pipeline.annotate(annotation);
				
				for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
					  Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
					  int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
					  //System.err.println(sentence);
					  //System.err.println("  Predicted sentiment: " + sentimentString(sentiment));
					  
					  if (sentiment == 0)
						  vneg++;
					  else if( sentiment == 1) 
						  neg++;
					  else if (sentiment == 2) 
						  neutral++;
					  else if(sentiment == 3) 
						  pos++;
					  else if (sentiment == 4)
						  vpos++;
					}
				
				
				//System.out.println(lead_para[i]);
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
		// Object docs = (JSONObject) response.get("docs");
		// JSONObject headline = docs.getJSONObject("headline");

		// System.out.println(headline.toString());
		// System.out.println(docs.toString());
		
		System.out.println("V Negative" + vneg);
		System.out.println("Negative" + neg);
		System.out.println("Neutral" + neutral);
		System.out.println("Positive" + pos);
		System.out.println("V Positive" + vpos);
	}
	
	
}
