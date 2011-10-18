package org.zoodb.jdo.internal.client;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;

/**
 * This bundles the unlikely friends Class, Node and Session.
 * 
 * This is primarily an optimization, such that every persistent capable object PC needs only
 * one reference (to ClassNodeSessionBundle) instead of three to each of the above. At the moment, 
 * this saves only 16byte per PC, but that is already considerable in cases with many little 
 * objects (SNA: 50.000.000 PC -> saves 800MB).
 * 
 * TODO
 * In future this may also contain class extents per node, as required by the commit(), 
 * evict(class) or possibly query methods.
 * 
 * @author Tilmann Zäschke
 */
public final class ClassNodeSessionBundle {

	private final Session session;
	private final Node node;
	private final ZooClassDef def;
	
	public ClassNodeSessionBundle(ZooClassDef def, Session session, Node node) {
		this.def = def;
		this.session = session;
		this.node = node;
	}
	
	public final Session getSession() {
		return session;
	}
	
	public final Node getNode() {
		return node;
	}
	
	public final ZooClassDef getClassDef() {
		return def;
	}
}
