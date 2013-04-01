package org.zoodb.profiling.pop1;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;
import org.zoodb.profiling.DBLPUtils;
import org.zoodb.profiling.model1.Author;
import org.zoodb.profiling.model1.AuthorContact;
import org.zoodb.profiling.model1.Conference;
import org.zoodb.profiling.model1.Publication;

public class ConfigHandler extends DefaultHandler {
	
	public static Set<Publication> newPS= new HashSet(650000);
    public static Map<String,Author> authorMap = new HashMap<String,Author>(600000); 
    public static Map<String,Conference> conferenceMap = new HashMap<String,Conference>(20000);

    private Locator locator;


    private String key;
    private String title;
    private String year;
    private String authorName;
    private String crossRef;
    private List<Author> authorForPublication = new LinkedList<Author>();
    private String currentElement;
    private int rating;
    
    private int counter = 0;
    private boolean insideInproceedings;
    private boolean insideProceedings;
    private boolean insidePerson;
    private boolean insideTitle;
    private boolean insideAuthor;
    private boolean insideYear;
    private boolean insideCrossRef;
    private boolean insideRating;
    
    private String[] unwantedElems = new String[] {"article","book","incollection","phdthesis","mastersthesis","www"};

    public void setDocumentLocator(Locator locator) {
        this.locator = locator;
    }

    public void startElement(String namespaceURI, String localName, String rawName, Attributes atts) throws SAXException {
    	// we do not want to inspect these elements
    	for (int i=0;i<unwantedElems.length;i++) {
    		if (rawName.equalsIgnoreCase(unwantedElems[i])) {
    			currentElement = "";
    			return;
    		}
    	}
    	
    	// are we in a 'proceeding' or in an 'inproceeding'
    	if (rawName.equals("inproceedings") || rawName.equals("proceedings")) {
    		currentElement = rawName;
    		
    		// we do have a key for sure
       		key = atts.getValue("key");
    	}
  	
    	if (insideInproceedings = (rawName.equalsIgnoreCase("inproceedings"))) {
    		authorForPublication = new LinkedList<Author>();
 
    	} else if (insideAuthor = (rawName.equalsIgnoreCase("author"))) {
    		authorName = "";
    	} else if (insideTitle = (rawName.equalsIgnoreCase("title"))) {
    		title = "";
    	} else if (insideYear = (rawName.equalsIgnoreCase("year"))) {
    		year = "";
    	} else if (insideCrossRef = (rawName.equalsIgnoreCase("crossref"))) {
    		crossRef = null;
    	} else if (insideRating = (rawName.equalsIgnoreCase("rating"))) {
    		rating = -1;
    	}
    	
    }

    public void endElement(String namespaceURI, String localName, String rawName) throws SAXException {

    	//if (rawName.equals("author") || rawName.equals("editor")) {
    	if (rawName.equals("author") && currentElement.equalsIgnoreCase("inproceedings") ) {

    		Author a = authorMap.get(authorName);
    		if (a == null) {
    			a = new Author();
    			a.setName(authorName);
    			//uncomment these lines when populating for the un-optimized model
    			AuthorContact ad = new AuthorContact();
    			ad.setEmail(DBLPUtils.getRandomString(15));
    			ad.setUniversity(DBLPUtils.getRandomString(30));
    			a.setDetails(ad);
    			//end un-optimized model
    			
    			//uncomment these lines when populating for the optimized model
//    			a.setEmail(DBLPUtils.getRandomString(15));
//    			a.setUniversity(DBLPUtils.getRandomString(30));
    			//end optimized model

    			authorMap.put(authorName, a);

    			if (authorForPublication == null) authorForPublication = new LinkedList<Author>();

    			authorForPublication.add(a);
    		} else {
    			// a is not null and already exists
    			if (authorForPublication == null) authorForPublication = new LinkedList<Author>();
    			authorForPublication.add(a);
    		}
    		return;
    		
        } else if (rawName.equalsIgnoreCase("inproceedings") ) {
        	if (crossRef != null && counter < 20000000) {
	        	// create new publication objects
	        	Publication p = new Publication();
	        	
	        	//uncomment the following line for populating the optimized model
//	        	p.setPs(new PublicationSplit());
//	        	p.setpAbstract(new PublicationAbstract());
				//end optimized model
				
	        	p.setKey(key);
	        	p.setTitle(title);
	        	p.setYear(Integer.valueOf(year));
	        	p.setRating(DBLPUtils.getRating());
	        	Author b;
	        	for (Author a : authorForPublication) {
	        		b= a;
	        		try {
	        			a.addPublication(p);
					} catch (Exception e) {
						e.printStackTrace();
					}
	        	}
	        	
	        	// for each of the authors in authorList (add p to their publications list
	        	try {
					p.setTargetA(authorForPublication);
					
					newPS.add(p);
					counter++;
				} catch (Exception e) {
					//e.printStackTrace();
				}
	        	
	        	//empty authorForPublication for the next publication
	        	authorForPublication = null;
	        	
	        	//add this publication to the conference, create conference if necessary
	        	Conference c  = conferenceMap.get(crossRef);
	        		
	        	try {
	        		if (c != null) {
	        			c.addPublications(p);
	        			p.setConference(c);
	        		} else {
	        			c = new Conference();
	        			c.setKey(crossRef);
	        			c.addPublications(p);
	        			p.setConference(c);
	        			conferenceMap.put(crossRef, c);
	        			
	        		}
	        	} catch (Exception e) {
	        		e.printStackTrace();
	        	}
        	}
        	
        	
        } else if (rawName.equalsIgnoreCase("proceedings")) {
        	if (key != null) {
        		Conference c = conferenceMap.get(key);
        		
        		if (c == null) {
        			c = new Conference();
        			c.setYear(Integer.valueOf(year));
        			c.setIssue(title);
        			c.setKey(key);
        			conferenceMap.put(key, c);
        		} else {
        			//conference was already in the map (set via a previous inproceedings-element), set year and title
        			c.setYear(Integer.valueOf(year));
        			c.setIssue(title);
        		}
        	}
        }
    }

    public void characters(char[] ch, int start, int length) throws SAXException {
        if (currentElement.equals("inproceedings") || currentElement.equals("proceedings")) {
        	
        	if (insideAuthor) {
        		authorName = new String(ch, start, length);
        	} else if (insideTitle) {
        		title = new String(ch, start, length);
        	} else if (insideYear) {
        		year = new String(ch, start, length);
        	} else if (insideCrossRef) {
        		crossRef = new String(ch, start, length);
        	} else if (insideRating) {
        		rating = Integer.valueOf(new String(ch, start, length));
        	}
        }
    }

    private void Message(String mode, SAXParseException exception) {
        System.out.println(mode + " Line: " + exception.getLineNumber() + " URI: " + exception.getSystemId() + "\n" + " Message: "  + exception.getMessage());
    }

    public void warning(SAXParseException exception) throws SAXException {
        Message("**Parsing Warning**\n", exception);
        throw new SAXException("Warning encountered");
    }

    public void error(SAXParseException exception) throws SAXException {
        Message("**Parsing Error**\n", exception);
        throw new SAXException("Error encountered");
    }

    public void fatalError(SAXParseException exception) throws SAXException {
        Message("**Parsing Fatal Error**\n", exception);
        throw new SAXException("Fatal Error encountered");
    }
} 


