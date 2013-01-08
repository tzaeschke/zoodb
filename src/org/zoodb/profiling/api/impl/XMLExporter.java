package org.zoodb.profiling.api.impl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;

import org.zoodb.profiling.api.IDataExporter;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

import ch.ethz.globis.profiling.commons.query.AbstractQuery;
import ch.ethz.globis.profiling.commons.query.JDOQuery;
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
		Collection<AbstractQuery> apiQueries = transform2APIQuery(queries);
		
		FileOutputStream fos = null;
		pfn = "profiler_queries_" + sdf.format(start) + "-" + sdf.format(end) + ".xml";
		try {
			fos = new FileOutputStream(pfn);
			
			XStream xstream = new XStream(new DomDriver("UTF-8"));
			xstream.toXML(apiQueries,fos);
			fos.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Transforms profiled query information (QueryProfile) to API standard (JDOQuery) 
	 * @param queries
	 * @return
	 */
	private Collection<AbstractQuery> transform2APIQuery(Collection<QueryProfile> queries) {
		Collection<AbstractQuery> result = new LinkedList<AbstractQuery>();
		
		JDOQuery jq = null;
		for (QueryProfile qp : queries) {
			jq = new JDOQuery();
			
			jq.setCandidateClassName(qp.getCandidateClass().getName());
			
			if (qp.getResultClass() != null) {
				jq.setResultClassName(qp.getResultClass().getName());
			}
			
			jq.setUnique(qp.isUnique());
			jq.setExcludeSubclasses(qp.isExcludeSubclasses());
			jq.setImports(qp.getImportClause());
			jq.setResult(qp.getResult());
			jq.setFilter(qp.getFilterClause());
			jq.setGrouping(qp.getGroupClause());
			jq.setOrdering(qp.getOrderClause());
			jq.setVariables(qp.getVariables());
			
			Map<String,Integer> execCounts = qp.getExecutionCounts();
			Map<String,Long> execTimes = qp.getExecutionTimes();
			String[] trxs = (String[]) execCounts.keySet().toArray(new String[execCounts.keySet().size()]);
			
			int trxCount = trxs.length;
			int[] executionCounts = new int[trxCount];
			long[] executionTimes = new long[trxCount];
			
			for (int i=0;i<trxCount;i++) {
				executionCounts[i]	= execCounts.get(trxs[i]);
				executionTimes[i]	= execTimes.get(trxs[i]);
			}
			
			jq.setTrx(trxs);
			jq.setExecutionCount(executionCounts);
			jq.setExecutionTime(executionTimes);

			
			result.add(jq);
		}
		
		return result;
	}

}
