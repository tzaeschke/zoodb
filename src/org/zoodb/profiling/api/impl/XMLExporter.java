package org.zoodb.profiling.api.impl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.zoodb.profiling.api.IDataExporter;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class XMLExporter implements IDataExporter {
	
	private String pfn;
	
	private Date start;
	private Date end;
	
	private SimpleDateFormat sdf = new SimpleDateFormat("ddMMMyy-HHmm");
	
	public XMLExporter(Date start,Date end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public void exportSuggestions(Collection<AbstractSuggestion> suggestions) {	
		FileOutputStream fos = null;
		pfn = "profiler_suggestions_" + sdf.format(start) + "-" + sdf.format(end) + ".xml";
		try {
			fos = new FileOutputStream(pfn);
			
			XStream xstream = new XStream(new DomDriver("UTF-8"));
			xstream.toXML(suggestions,fos);
			fos.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void exportQueries(Collection<QueryProfile> queries) {
		FileOutputStream fos = null;
		pfn = "profiler_queries_" + sdf.format(start) + "-" + sdf.format(end) + ".xml";
		try {
			fos = new FileOutputStream(pfn);
			
			XStream xstream = new XStream(new DomDriver("UTF-8"));
			xstream.toXML(queries,fos);
			fos.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
