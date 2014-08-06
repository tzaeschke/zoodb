package org.zoodb.profiling.api;

import java.util.Collection;

import org.zoodb.profiling.api.impl.QueryProfile;

import ch.ethz.globis.profiling.commons.statistics.ClassStatistics;
import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public interface IDataExporter {
	
	public void exportSuggestions(Collection<AbstractSuggestion> suggestions);
	
	public void exportQueries(Collection<QueryProfile> queries);
	
	public void exportClassStatistics(Collection<ClassStatistics> classStatistics);

}
