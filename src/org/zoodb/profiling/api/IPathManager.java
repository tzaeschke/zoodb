package org.zoodb.profiling.api;

import java.util.Iterator;

import org.zoodb.profiling.api.impl.ActivationArchive;


public interface IPathManager {
	
	public void add(AbstractActivation a);
	
	public ActivationArchive getArchive(Class<?> c);
	
	public Iterator<Class<?>> getClassIterator();
	
}