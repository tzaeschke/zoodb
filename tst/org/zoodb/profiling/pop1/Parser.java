package org.zoodb.profiling.pop1;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.profiling.DBUtils;
import org.zoodb.profiling.model1.Author;
import org.zoodb.profiling.model1.AuthorContact;
import org.zoodb.profiling.model1.Conference;
import org.zoodb.profiling.model1.ConferenceSeries;
import org.zoodb.profiling.model1.Publication;
import org.zoodb.profiling.model1.PublicationAbstract;
import org.zoodb.profiling.model1.PublicationSplit;
import org.zoodb.profiling.model1.Tags;

public class Parser {
	
	static final String DB_NAME = "dblpV1b";
	
	private Map<String,ConferenceSeries> conferenceSeries = new HashMap<String,ConferenceSeries>(10000);
   
	public Parser(String uri) {
		try {
			SAXParserFactory parserFactory = SAXParserFactory.newInstance();
			SAXParser parser = parserFactory.newSAXParser();
			ConfigHandler handler = new ConfigHandler();
			parser.getXMLReader().setFeature("http://xml.org/sax/features/validation", true);
			parser.parse(new File(uri), handler);

		} catch (IOException e) {
			System.out.println("Error reading URI: " + e.getMessage());
		} catch (SAXException e) {
			System.out.println("Error in parsing: " + e.getMessage());
		} catch (ParserConfigurationException e) {
			System.out.println("Error in XML parser configuration: " + e.getMessage());
		}


		DBUtils.createDB(DB_NAME);
		PersistenceManager pm = DBUtils.openDB(DB_NAME);

		pm.currentTransaction().begin();
		ZooSchema.defineClass(pm, Author.class);
		ZooSchema.defineClass(pm, AuthorContact.class);
		ZooSchema.defineClass(pm, Publication.class);
		ZooSchema.defineClass(pm, Tags.class);
		ZooSchema.defineClass(pm, Conference.class);
		ZooSchema.defineClass(pm, ConferenceSeries.class);
		ZooSchema.defineClass(pm, PublicationAbstract.class);
		ZooSchema.defineClass(pm, PublicationSplit.class);



		Iterator<Conference> iter = ConfigHandler.conferenceMap.values().iterator();

		Conference currentConference = null;
		while (iter.hasNext()) {
			currentConference = iter.next();
			String key = currentConference.getKey();
			
			if (key.startsWith("conf/")) {
				String[] parts = key.split("/");

				if (parts.length >= 2) {
					String seriesKey = parts[1];

					ConferenceSeries series = conferenceSeries.get(seriesKey);
					try {
						if (series != null) {
							series.addConferences(currentConference);
							currentConference.setSeries(series);
						} else {
							series = new ConferenceSeries();
							series.setDBLPkey(seriesKey);
							series.addConferences(currentConference);
							currentConference.setSeries(series);
							conferenceSeries.put(seriesKey, series);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				pm.makePersistent(currentConference);
			} else {
				continue;
			}
		}
		
		System.out.println("Number of Persons : " + ConfigHandler.authorMap.size());
		System.out.println("Number of Publications (Inproceedings): " + ConfigHandler.newPS.size()); 
		System.out.println("Number of Conferences (Proceedings): " + ConfigHandler.conferenceMap.size());
		System.out.println("Number of Conference-Series: " + conferenceSeries.size());


		System.out.println("Starting commit..");
		pm.currentTransaction().commit();
		DBUtils.closeDB(pm);
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("Usage: java Parser [input]");
			System.exit(0);
		}
		new Parser(args[0]);
		
		String[] args2 = {DB_NAME};
		
		AbstractPopulator.main(args2);
		System.out.println("Finished.");
	}
}


