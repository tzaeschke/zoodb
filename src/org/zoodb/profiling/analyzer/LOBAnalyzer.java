package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.LinkedList;

import org.zoodb.profiling.ProfilingConfig;
import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.impl.ClassSizeManager;
import org.zoodb.profiling.api.impl.LobCandidate;
import org.zoodb.profiling.api.impl.ProfilingManager;
import org.zoodb.profiling.suggestion.SuggestionFactory;

import ch.ethz.globis.profiling.commons.suggestion.AbstractSuggestion;

/**
 * Analyzes classes for blob-suggestions
 * @author tobias
 *
 */
public class LOBAnalyzer implements IAnalyzer {
	
	private IFieldManager fm;
	private ClassSizeManager csm;
	
	public LOBAnalyzer() {
		fm = ProfilingManager.getInstance().getFieldManager();
		csm = ProfilingManager.getInstance().getClassSizeManager();
	}

	@Override
	public Collection<AbstractSuggestion> analyze(Collection<AbstractSuggestion> suggestions) {
		Collection<AbstractSuggestion> result = new LinkedList<AbstractSuggestion>();
		
		
		
		/*
		 * Go through all LobCandidates. We know that for a LobCandidate, it holds
		 * that at least once, the attribute was detected as a Lob.
		 */
		for (LobCandidate lc : fm.getLOBCandidates()) {
			
			//total activations for this class
			int totalActivations = ProfilingManager.getInstance().getPathManager().getArchive(lc.getClazz()).size();
			
			Collection<Field> fields = lc.getFields();
			
			for (Field f : fields) {
				//how many times did we access this field?
				//(this includes write-access, for a write-access, we also had to load it first!)
				int totalAccessCount = fm.get(lc.getClazz(),f.getName(),null);
				//how many times did we detect this field as a lob?
				int detectionCount = lc.getDetectionsByField(f);
				//avg-size of this field
				Double avgSize = csm.getClassStats(lc.getClazz()).getAvgFieldSizeForField(f.getName());
				
				if (avgSize > ProfilingConfig.LOB_TRESHOLD) {
					//avgSize indicates that this field is a lob
					
					//check the detection/activation ratio, if the detection ratio is greater than threshold
					//the highter this ratio (closer to 1), the more objects of this class
					
					double da_ratio = detectionCount / (double) totalActivations;
					
					if (da_ratio  >= ProfilingConfig.LOB_THRESHOLD_DA_RATIO) {
						/*
						 * At least 10 %  of the activations of objects of this field, are greater than the threshold
						 * Attention: in theory, all activations could be triggered by the same object.
						 */
						
						double accessDetectRatio = totalAccessCount / (double) detectionCount;
						
						if (accessDetectRatio == 1) {
							//every time a blob was detected, it was accessed (90% true, one could have accessed 
							//the field in these cases where no blob-state was detected for this attribute
						} else if (accessDetectRatio > 1) {
							//same reasoning as above
						} else {
							//we have at least one case where a blob-attribute was not accessed
							
							//how many times the blob has been activated, but not been accessed 
							//--> overhead: blob was transferred to memory
							int activationsNoLobRead = totalActivations-totalAccessCount;
							
							//how many times other attributes have been written but not the blob 
							//--> overhead: blob was transferred back to disk
							int otherWritesNoLobWrite = 0;
							
							//how many times the blob was accessed but no other attribute on the same object
							//--> overhead: sizeof(C)-avgBlobSize were transferred into memory
							int lobAccessNoOtherAccess = 0;
							
							//how many times the blob was written but no other attributes
							//-->overhead: sizeof(C)--avgBlobSize were transferred back to disk
							int lobWriteNoOtherWrite = 0;
							
							//bring the suggestion anyway, it will be decided at a later point whether it will be exported
							result.add(SuggestionFactory.getLS(lc.getClazz(), f, detectionCount, totalAccessCount));
							
						}
						
					}
					
					
					
				}
				
				
				
				
				
				
			}
		}
		suggestions.addAll(result);
		return result;
	}

}
