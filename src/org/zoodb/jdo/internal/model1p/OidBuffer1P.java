package org.zoodb.jdo.internal.model1p;

import org.zoodb.jdo.internal.OidBuffer;

public class OidBuffer1P extends OidBuffer {

	private Node1P node;
	
	public OidBuffer1P(Node1P node) {
		this.node = node;
	}

	@Override
	public long[] allocateMoreOids() {
		return node.getDiskAccess().allocateOids(this.getOidAllocSize());
	}
	
}
