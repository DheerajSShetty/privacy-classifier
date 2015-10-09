package edu.ncsu.csc.privacy.parser;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

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

public class NewsSentiment {

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
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	    String parserModel = null;
		String sentimentModel = null;

		String filename = null;

		//tweetSentiment();
		  
		Properties props = new Properties();
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
			}
		
	}

}
