package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zoodb.profiling.api.AbstractActivation;
import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.tree.impl.AbstractNode;
import org.zoodb.profiling.api.tree.impl.ClazzNode;
import org.zoodb.profiling.api.tree.impl.NodeTraverser;
import org.zoodb.profiling.api.tree.impl.ObjectNode;



public class PathManagerTreeV2 implements IPathManager {
	
	private List<ObjectNode> objectLevelTrees;
	private List<ClazzNode> classLevelTrees;
	private Map<Class<?>,ActivationArchive> classArchives;
	
	private Logger logger = LogManager.getLogger("allLogger");
	
	public PathManagerTreeV2() {
		objectLevelTrees = new LinkedList<ObjectNode>();
		classLevelTrees = new LinkedList<ClazzNode>();
		classArchives = new HashMap<Class<?>,ActivationArchive>();
	}
	
	public void add(AbstractActivation a) {
		ActivationArchive aa = classArchives.get(a.getClazz());
		
		if (aa == null) {
			aa = new ActivationArchive();
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