package org.zoodb.profiling.analyzer;

import org.zoodb.profiling.api.impl.ProfilingManager;

import ch.ethz.globis.profiling.commons.suggestion.FieldCount;

/**
 * Strategy to split a class into 2 parts.
 * Part1 is a read-only attribute set. Part2 is a write-only dataset.
 * This class tries to find a split in the following way:
 * 	compare the number of writes (=tw) to this field to the total number of activations of this class.
 * 		if tw is greater than a specified threshold, it should be outsourced
 * 
 * So we check for the first index, which entry is smaller than the specified threshold
 * 
 * @author tobiasg
 *
 */
public class ReadWriteSplitStrategy implements ISplitStrategy {
	
	@Override
	public int getSplitIndex(FieldCount[] fc, Class<?> c) {
		//Remember: at this point, fcs is sorted in descending order by the number of writes

		int totalActivations = ProfilingManager.getInstance().getPathManager().getArchive(c).size();
		
		int fieldCount = fc.length;
		for (int i=0;i<fieldCount;i++) {
			int writeCount = fc[i].getCount();
			
			double ratio = writeCount*1.0d / totalActivations; 
			
			//by setting the threshold to 0, we can enforce a split in true disjunct read/write sets
//			if (ratio <= ProfilingConfig.SA_MIN_WRITE_THRESHOLD ) {
//				return i == 0 ? -1 : i;
//			} else {
//				continue;
//			}
			if (writeCount != 0) {
				continue;
			} else {
				return i == 0 ? -1 : i;
			}
			
		}

		return -1;
	}

}
