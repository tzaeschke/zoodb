/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.jdo.internal.client;

import org.zoodb.jdo.internal.DataDeleteSink;
import org.zoodb.jdo.internal.DataEvictor;
import org.zoodb.jdo.internal.DataIndexUpdater;
import org.zoodb.jdo.internal.DataSink;
import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;

/**
 * This bundles Class, Node and Session to a context for persistent objects.
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
public final class PCContext {

	private final Session session;
	private final Node node;
	private final ZooClassDef def;
	private final DataEvictor evictor;
	private final DataIndexUpdater updater;
    private final DataSink dataSink;
    private final DataDeleteSink dataDeleteSink;
	
	public PCContext(ZooClassDef def, Session session, Node node) {
		this.def = def;
		this.session = session;
		this.node = node;
		//only for non-schema classes
		if (def != null) {
			this.evictor = new DataEvictor(def, 
					session.getPersistenceManagerFactory().getEvictPrimitives());
			this.updater = new DataIndexUpdater(def);
		} else {
			this.evictor = null;
			this.updater = null;
		}
        //==null for schema bootstrapping   TODO why?
		if (node != null) {
		    dataSink = node.createDataSink(def);
		    dataDeleteSink = node.createDataDeleteSink(def);
		} else {
		    dataSink = null;
		    dataDeleteSink = null;
		}
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
	
	public final DataEvictor getEvictor() {
		return evictor;
	}

	public final DataIndexUpdater getIndexer() {
		return updater;
	}

    public DataSink getDataSink() {
        return dataSink;
    }

    public DataDeleteSink getDataDeleteSink() {
        return dataDeleteSink;
    }
}
