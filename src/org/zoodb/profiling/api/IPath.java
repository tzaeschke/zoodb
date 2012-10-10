package org.zoodb.profiling.api;

import java.util.Collection;

public interface IPath {
	
	/**
	 * @param a
	 */
	public void addActivationNode(Activation a);
	
	/**
	 * @return
	 */
	public Activation getTail();
	
	/**
	 * @return
	 */
	public Collection<Activation> getActivationNodes();
	
	

}
