package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPath;
import org.zoodb.profiling.api.IPathManager;


public class PathManager implements IPathManager {
	
	private List<IPath> paths;
	
	public PathManager() {
		paths = new LinkedList<IPath>();
	}

	@Override
	public void addActivationPathNode(Activation a, Object predecessor) {
		if (predecessor == null) {
			//start of a new path
			Path path = new Path(a);
			paths.add(path);
		} else {
			// Append this activation to the path 'x' where the tail of x is the predecessor of 'a'
			for (IPath p : paths) {
				if (p.getTail().getActivator() == predecessor) {
					p.addActivationNode(a);
					break;
				}
			}
		}
	}

	@Override
	public List<IPath> getPaths() {
			return paths;
	}

	@Override
	public void prettyPrintPaths() {
		for (IPath p : paths) {
	    	System.out.println("Starting new path...");
	    	Collection<Activation> activations = p.getActivationNodes();
	    	for (Activation a : activations) {
	    		System.out.println(a.prettyString());
	    	}
	    }
	}

	@Override
	public void aggregateObjectPaths() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prettyPrintClassPaths() {
		// TODO Auto-generated method stub
		
	}

}
