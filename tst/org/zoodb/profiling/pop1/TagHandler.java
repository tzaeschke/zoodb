package org.zoodb.profiling.pop1;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class TagHandler extends DefaultHandler {
	
	public static Map<String,List<String>> tag2Keys = new HashMap<String,List<String>>(600000);
	
	private String tagName;
	private String key;
	
	public void startElement(String namespaceURI, String localName, String rawName, Attributes atts) throws SAXException {
		key = atts.getValue("key");		
	}
	
    public void endElement(String namespaceURI, String localName, String rawName) throws SAXException {
    	if (key != null) {
    		List<String> tmp = tag2Keys.get(tagName);
    		
    		if (tmp == null) {
    			tmp = new LinkedList<String>();
    			tmp.add(key);
    			tag2Keys.put(tagName, tmp);
    		} else {
    			tmp.add(key);
    		}
    		
    	}
    }
    
    public void characters(char[] ch, int start, int length) throws SAXException {
    	tagName = new String(ch,start,length);
    }

}
