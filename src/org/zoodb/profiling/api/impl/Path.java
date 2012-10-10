package org.zoodb.profiling.api.impl;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.IPath;

public class Path implements IPath {
	
	private Activation root;
	private Activation tail;
	
	private List<Activation> nodes;
	
	public Path(Activation a) {
		this.root = a;
		this.tail = a;
		nodes = new LinkedList<Activation>();
		nodes.add(root);
	}
	
	public void addActivationNode(Activation a) {
		nodes.add(a);
		tail = a;
	}
	
	public Collection<Activation> getActivationNodes() {
		return nodes;
	}
	
	public Activation getTail() {
		return tail;
	}
	
}
