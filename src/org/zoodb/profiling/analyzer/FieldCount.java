package org.zoodb.profiling.analyzer;

public class FieldCount implements Comparable<FieldCount> {
	
	private String name;
	private int count;
	
	public FieldCount(String name,int count) {
		this.name = name;
		this.count = count;
	}

	@Override
	public int compareTo(FieldCount other) {
		return other.count - this.count;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

}