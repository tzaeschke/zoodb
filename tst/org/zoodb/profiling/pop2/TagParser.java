package org.zoodb.profiling.pop2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.zoodb.profiling.DBUtils;
import org.zoodb.profiling.model2.Publication;
import org.zoodb.profiling.model2.Tags;

public class TagParser {
	
	private Map<String, Publication> publications;
	
	public TagParser(String uri) {
	      try {
		     SAXParserFactory parserFactory = SAXParserFactory.newInstance();
		     SAXParser parser = parserFactory.newSAXParser();
		     TagHandler handler = new TagHandler();
		     
		     InputSource is = new InputSource();
		     is.setCharacterStream(new InputStreamReader(new FileInputStream(new File(uri))));
		     
		     parser.parse(is, handler);	         
	         
	      } catch (IOException e) {
	         System.out.println("Error reading URI: " + e.getMessage());
	         e.printStackTrace();
	      } catch (SAXException e) {
	         System.out.println("Error in parsing: " + e.getMessage());
	      } catch (ParserConfigurationException e) {
	         System.out.println("Error in XML parser configuration: " + e.getMessage());
	      }

	      System.out.println(TagHandler.tag2Keys.size());

	      //update publications and set tags
	      PersistenceManager pm = DBUtils.openDB("dblp");
	      
	      pm.currentTransaction().begin();
	      publications = initMap(pm.getExtent(Publication.class));

	      //create tag objects and update publications
	      for (String tag : TagHandler.tag2Keys.keySet()) {
	    	  List<String> keysAssoc = TagHandler.tag2Keys.get(tag);
	    	  
	    	  if (!keysAssoc.isEmpty()) {
	    		  Tags t = new Tags();
		    	  t.setLabel(tag);
		    	  
		    	  //get the publications for the keys and update them
		    	  for (String key : keysAssoc) {
		    		Publication p = publications.get(key);
		    		
		    		if (p != null) {
		    			t.addPublication(p);
		    			p.addTag(t);
		    			pm.makePersistent(t);
		    		}
		    	  }
		    	  
	    	  }
	      }
	      
	      
	      
	      pm.currentTransaction().commit();
	      
	}
	

	private Map<String, Publication> initMap(Extent<Publication> extent) {
		Map<String, Publication> publications = new HashMap<String, Publication>(50000);
		
		for (Publication p : extent) {
			publications.put(p.getKey(), p);
		}
		
		return publications;
	}
	
	public static void main(String[] args) {
	      if (args.length < 1) {
	         System.out.println("Usage: java Parser [input]");
	         System.exit(0);
	      }
	      TagParser tp = new TagParser(args[0]);
	   }
}
