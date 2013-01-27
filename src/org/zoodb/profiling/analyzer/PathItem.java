package org.zoodb.profiling.analyzer;

public class PathItem {
	
	Class<?> c;
	String fieldName;
	int traversalCount;
	
	
	public Class<?> getC() {
		return c;
	}
	public void setC(Class<?> c) {
		this.c = c;
	}
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public int getTraversalCount() {
		return traversalCount;
	}
	public void setTraversalCount(int traversalCount) {
		this.traversalCount = traversalCount;
	}
	
	
	public boolean equals(PathItem other) {
		return other.c == this.c && other.fieldName.equals(this.fieldName);
	}
	
	public void incVisitCounter() {
		traversalCount++;
		
	}

}