package org.zoodb.profiling.api.impl;

import org.zoodb.profiling.api.IFieldManager;
import org.zoodb.profiling.api.IPathManager;
import org.zoodb.profiling.api.IProfilingManager;

/**
 * @author tobiasg
 *
 */
public class ProfilingManager implements IProfilingManager {
	
	private static ProfilingManager singleton = null;
	
	private IPathManager pathManager;
	private IFieldManager fieldManager;
	
	
	public static ProfilingManager getInstance() {
		if (singleton == null) {
			singleton = new ProfilingManager();
		}
		return singleton;
	}
	
	private ProfilingManager() {
		pathManager = new PathManagerTree();
		fieldManager = new FieldManager();
	}
	
	@Override
	public void save() {
		// TODO Auto-generated method stub
	}

	@Override
	public IPathManager getPathManager() {
		return pathManager;
	}

	@Override
	public IFieldManager getFieldManager() {
		return fieldManager;
	}




	


}
