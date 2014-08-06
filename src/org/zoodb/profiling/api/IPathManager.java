package org.zoodb.profiling.api;

import java.util.Iterator;

import org.zoodb.internal.ZooClassDef;
import org.zoodb.profiling.api.impl.ActivationArchive;


public interface IPathManager {
	
	public void add(AbstractActivation a, ZooClassDef classDef);
	
	public ActivationArchive getArchive(Class<?> c);
	
	public Iterator<Class<?>> getClassIterator();

	public void addClass(ZooClassDef c);
	
}