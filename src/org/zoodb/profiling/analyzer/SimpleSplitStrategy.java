package org.zoodb.profiling.analyzer;

import ch.ethz.globis.profiling.commons.suggestion.FieldCount;

/**
 * Checks for a simple split. A simple split is a split where a set of attributes does not have any field-accesses
 * @author tobiasg
 *
 */
public class SimpleSplitStrategy implements ISplitStrategy {

	@Override
	public int getSplitIndex(FieldCount[] fc, Class<?> c) {
		int size = fc.length;
		
		//no split?
		if (fc[0] == fc[size-1]) {
			return -1;
		} else {
			//get the first index where fieldCount is zero
			for (int i=0;i<size;i++){
				if (fc[i].getCount() == 0) {
					return i;
				}
			}
			
		}
		return -1;
	}

}
