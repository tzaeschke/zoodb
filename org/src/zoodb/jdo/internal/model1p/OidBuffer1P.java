package org.zoodb.jdo.internal.model1p;

import org.zoodb.jdo.internal.OidBuffer;

public class OidBuffer1P extends OidBuffer {

	private Node1P _node;
	
	public OidBuffer1P(Node1P node) {
		_node = node;
	}

	@Override
	public long[] allocateMoreOids() {
		return _node.getDiskAccess().allocateOids(this.getOidAllocSize());
	}
	
}
