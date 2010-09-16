package org.zoodb.jdo.internal.model1p;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.ZooFactory;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;

public class Factory1P extends ZooFactory {

	@Override
	public Node createNode(String nodePath, ClientSessionCache cache) {
		return new Node1P(nodePath, cache);
	}

}
