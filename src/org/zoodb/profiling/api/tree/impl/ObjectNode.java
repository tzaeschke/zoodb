package org.zoodb.profiling.api.tree.impl;

import org.zoodb.profiling.api.Activation;
import org.zoodb.profiling.api.ObjectFieldStats;

public class ObjectNode extends AbstractNode {
	
	private Activation activation;
	
	private String objectId;
	
	private ObjectFieldStats objectFieldStats;
	
	public ObjectNode(Activation activation) {
		super();
		this.activation = activation;
	}

	@Override
	public String toString() {
		return activation.prettyString();
	}

	public String getObjectId() {
		return objectId;
	}

	public void setObjectId(String objectId) {
		this.objectId = objectId;
	}

	public Activation getActivation() {
		return activation;
	}
	
	/**
	 * Returns the first ObjectNode in the subtree with root=this which has the same class and same objectId
	 * @param clazzName
	 * @param oid
	 * @return
	 */
	public ObjectNode getNode(String clazzName, String oid) {
		if (this.clazzName.equals(clazzName) && this.objectId.equals(oid)) {
			return this;
		} else {
			if (children.size() > 0) {
				for ( AbstractNode child : children) {
					ObjectNode childResult = ((ObjectNode) child).getNode(clazzName,oid);
					
					if (childResult != null) {
						return childResult;
					}
				}
			} else {
				return null;
			}
		}
		return null;
	}
	
	

}
