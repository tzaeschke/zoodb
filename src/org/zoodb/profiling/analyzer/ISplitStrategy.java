package org.zoodb.profiling.analyzer;

public interface ISplitStrategy {
	
	/**
	 * Returns a positive number (index) where the fields should be split
	 * Returns a negative number if no split should be done
	 * @param fc
	 * @return
	 */
	public int getSplitIndex(FieldCount[] fc, Class<?> c);

}
