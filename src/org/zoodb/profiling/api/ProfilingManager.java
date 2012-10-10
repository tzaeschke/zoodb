package org.zoodb.profiling.api;

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
		//pathManager = new PathManager();
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
