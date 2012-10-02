package org.zoodb.profiling;

/**
 * @author tobiasg
 *
 */
public class ProfilingManager implements IProfilingManager {
	
	private static ProfilingManager singleton = null;
	
	private IPathManager pathManager;
	
	
	public static ProfilingManager getInstance() {
		if (singleton == null) {
			singleton = new ProfilingManager();
		}
		return singleton;
	}
	
	private ProfilingManager() {
		pathManager = new PathManager();
	}
	
	@Override
	public void save() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IPathManager getPathManager() {
		return pathManager;
	}




	


}
