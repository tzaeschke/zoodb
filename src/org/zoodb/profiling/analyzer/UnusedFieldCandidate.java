package org.zoodb.profiling.analyzer;

import java.lang.reflect.Field;

public class UnusedFieldCandidate {
	
	private Class<?> clazz;
	private Class<?> superClazz;
	private Field f;
	
	private int totalActivationsClazz;
	private int totalWritesClazz;
	
	
	public Class<?> getClazz() {
		return clazz;
	}
	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}
	public Class<?> getSuperClazz() {
		return superClazz;
	}
	public void setSuperClazz(Class<?> superClazz) {
		this.superClazz = superClazz;
	}
	public Field getF() {
		return f;
	}
	public void setF(Field f) {
		this.f = f;
	}
	public int getTotalActivationsClazz() {
		return totalActivationsClazz;
	}
	public void setTotalActivationsClazz(int totalActivationsClazz) {
		this.totalActivationsClazz = totalActivationsClazz;
	}
	public int getTotalWritesClazz() {
		return totalWritesClazz;
	}
	public void setTotalWritesClazz(int totalWritesClazz) {
		this.totalWritesClazz = totalWritesClazz;
	}
}