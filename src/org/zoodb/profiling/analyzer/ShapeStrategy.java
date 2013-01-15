package org.zoodb.profiling.analyzer;

/**
 * 
 * @author tobiasg
 *
 */
public class ShapeStrategy implements ISimilarityStrategy {

	@Override
	public boolean executeStrategy(int[] accessVector, TrxGroup g) {
		
		//check only the shape, not the count of each entry
		int[] referenceShape = g.getAccessVectors().get(0);
		
		int size = accessVector.length;
		
		for (int i=0;i<size;i++) {
			if (referenceShape[i] == 0 && accessVector[i] != 0) {
				return false;
			} else if (referenceShape[i] != 0 && accessVector[i] == 0) {
				return false;
			}
		}
		return true;
	}

}