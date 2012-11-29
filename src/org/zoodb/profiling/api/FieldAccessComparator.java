package org.zoodb.profiling.api;

import java.util.Comparator;

/**
 * Compares two fieldaccesses based on their timestamps
 *
 */
public class FieldAccessComparator implements Comparator<IFieldAccess> {

	@Override
	public int compare(IFieldAccess arg0, IFieldAccess arg1) {
		return (int) (arg0.getTimestamp()-arg1.getTimestamp());
	}

}
