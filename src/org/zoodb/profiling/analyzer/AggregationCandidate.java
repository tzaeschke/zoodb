package org.zoodb.profiling.analyzer;

import java.util.Collection;
import java.util.LinkedList;

import org.zoodb.profiling.api.impl.ProfilingManager;


/**
 * @author tobias
 *
 */
public class AggregationCandidate {
	
	/**
	 * Name of the class in which an aggregated field would be introduced 
	 */
	private String parentClass;
	
	/**
	 * Name of the class which contains the field upon which has been aggregated 
	 */
	private String assocClass;
	
	/**
	 * Name of the field in the collection-item class upon which has been aggregated 
	 */
	private String fieldName;
	
	/**
	 * The write set of on the parentClass: contains (oid,trx) tuples 
	 */
	private Collection<Object[]> parentWrites;
	
	private long bytes;
	
	private int itemCounter;
	
	/**
	 * The number of times the read pattern has been detected (2-level reads)  
	 */
	private int patternCounter;
	
	
	public AggregationCandidate(String parentClass, String fieldName, String assocClass) {
		this.parentClass = parentClass;
		this.fieldName = fieldName;
		this.assocClass = assocClass;
		
		parentWrites = new LinkedList<Object[]>();
	}
	
	/**
	 * Adds the pair (oid,trx) to the write set only if it does not exist yet (--> additional write)
	 * @param trx
	 * @param oid
	 */
	public void add2Writes(String trx, long oid) {
		for (Object[] o : parentWrites) {
			if (o[0].equals(oid) && o[1].equals(trx)) {
				return;
			}
		}
		parentWrites.add(new Object[] {oid,trx});
	}

	public String getParentClass() {
		return parentClass;
	}

	public void setParentClass(String parentClass) {
		this.parentClass = parentClass;
	}

	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	public Collection<Object[]> getParentWrites() {
		return parentWrites;
	}

	public void setParentWrites(Collection<Object[]> parentWrites) {
		this.parentWrites = parentWrites;
	}

	public String getAssocClass() {
		return assocClass;
	}

	public void setAssocClass(String assocClass) {
		this.assocClass = assocClass;
	}

	public long getBytes() {
		return bytes;
	}

	public void setBytes(long bytes) {
		this.bytes = bytes;
	}

	public int getItemCounter() {
		return itemCounter;
	}

	public void setItemCounter(int itemCounter) {
		this.itemCounter = itemCounter;
	}
	
	/** 
	 * Returns true if the gain is still higher than the cost (including the costs introduced by writes)
	 * @return
	 */
	public boolean evaluate() {
		int writeCount = parentWrites.size();
		try {
			int totalActivationsOfParent = ProfilingManager.getInstance().getPathManager().getArchive(Class.forName(parentClass)).size();
			
			if ( (totalActivationsOfParent + writeCount)*4 < bytes) {
				return true;
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return false;
	}

	public int getPatternCounter() {
		return patternCounter;
	}

	public void setPatternCounter(int patternCounter) {
		this.patternCounter = patternCounter;
	}
	
	
}