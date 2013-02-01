package org.zoodb.profiling.api.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.IPathManager;


public class PathManagerTreeV2 implements IPathManager {
	
	private Map<Class<?>,ActivationArchive> classArchives;
	
	private Logger logger = LogManager.getLogger("allLogger");
	
	public PathManagerTreeV2() {
		classArchives = new HashMap<Class<?>,ActivationArchive>();
	}
	
	public void add(AbstractActivation a, ZooClassDef classDef) {
		ActivationArchive aa = classArchives.get(a.getClazz());
		
		if (aa == null) {
			aa = new ActivationArchive(classDef);
			classArchives.put(a.getClazz(), aa);
		}
		aa.addItem(a);
	}

	
	
	@Override
	public ActivationArchive getArchive(Class<?> c) {
		return classArchives.get(c);
	}

	@Override
	public Iterator<Class<?>> getClassIterator() {
		return classArchives.keySet().iterator();
	}

}