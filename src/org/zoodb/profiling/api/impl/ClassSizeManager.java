package org.zoodb.profiling.api.impl;

import java.util.HashMap;
import java.util.Map;

import org.zoodb.jdo.internal.ZooClassDef;

public class ClassSizeManager {
	
	private Map<Class<?>, ClassSizeStats> clazzStats;
	
	public ClassSizeManager() {
		clazzStats = new HashMap<Class<?>, ClassSizeStats>();
	}
	
	public ClassSizeStats getClassStats(Class<?> c) {
		ClassSizeStats css = clazzStats.get(c);
		
		if (css == null) {
			ActivationArchive a = ProfilingManager.getInstance().getPathManager().getArchive(c);
			ZooClassDef def = a.getZooClassDef(); 
			css = new ClassSizeStats(def);
			clazzStats.put(c, css);
		}
		return css;
	}

}
