package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;

import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.impl.LobCandidate;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

public class LOBAnalyzer implements IAnalyzer {
	
	private IFieldManager fm;
	
	public LOBAnalyzer() {
		fm = ProfilingManager.getInstance().getFieldManager();
	}

	@Override
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions) {
		Collection<AbstractSuggestion> result = new LinkedList<AbstractSuggestion>();
		
		for (LobCandidate lc : fm.getLOBCandidates()) {
			
			Collection<Field> fields = lc.getFields();
			
			for (Field f : fields) {
				int totalAccessCount = fm.get(lc.getClazz(),f.getName(),null);
				int detectionCount = lc.getDetectionsByField(f);
				
				double ratio = detectionCount / (double) totalAccessCount;
				
				if (ratio > 1) {
					result.add(SuggestionFactory.getLS(lc.getClazz(), f, detectionCount, totalAccessCount));
				}
			}
		}
		suggestions.addAll(result);
		return result;
	}

}
