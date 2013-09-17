/*
 * Copyright 2009-2013 Tilmann Zaeschke. All rights reserved.
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

import java.util.Arrays;

import javax.jdo.JDOUserException;
import javax.jdo.listener.ClearLifecycleListener;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.DeleteLifecycleListener;
import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.jdo.listener.LoadLifecycleListener;
import javax.jdo.listener.StoreLifecycleListener;

import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.api.impl.ZooPCImpl;
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
 * @author Tilmann Zaeschke
 */
public final class PCContext {

	private final Session session;
	private final Node node;
	private final ZooClassDef def;
	private final DataEvictor evictor;
	private final DataIndexUpdater updater;
    private final DataSink dataSink;
    private final DataDeleteSink dataDeleteSink;
    private InstanceLifecycleListener[] listeners = null;
	
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

    
	public void addLifecycleListener(InstanceLifecycleListener listener) {
		if (listeners == null) {
			listeners = new InstanceLifecycleListener[0];
		}
		for (InstanceLifecycleListener l: listeners) {
			if (l.equals(listener)) {
				throw new JDOUserException("Listener already registered for class " + 
						def.getClassName());
			}
		}
		listeners = Arrays.copyOf(listeners, listeners.length + 1);
		listeners[listeners.length-1] = listener;
	}

	
	public void removeLifecycleListener(InstanceLifecycleListener listener) {
		if (listeners == null) {
			return;
		}
		
		for (int i = 0; i < listeners.length; i++) {
			InstanceLifecycleListener l = listeners[i];
			if (l.equals(listener)) {
				if (listeners.length > 1) {
					if (i + 1 < listeners.length) {
						System.arraycopy(listeners, i+1, listeners, i, listeners.length-i-1);
					}
					listeners = Arrays.copyOf(listeners, listeners.length -1);
				} else {
					listeners = null;
				}
				break;
			}
		}
	}

	public void notifyEvent(ZooPCImpl src, ZooInstanceEvent event) {
		if (def.getSuperDef() != null) {
			def.getSuperDef().getProvidedContext().notifyEvent(src, event);
		}
		if (listeners == null || src.getClass() == ZooClassDef.class) {
			//Ignore this for system objects, even if the listeners are stored in a system objects
			return;
		}
		//TODO this is a bit dirty, move classes into enum?
		for (InstanceLifecycleListener l: listeners) {
			switch (event) {
			case PRE_CLEAR: if (ClearLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((ClearLifecycleListener)l).preClear(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.CLEAR));
			}
			break;
			case POST_CLEAR: if (ClearLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((ClearLifecycleListener)l).postClear(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.CLEAR));
			}
			break; 
			case CREATE: if (CreateLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((CreateLifecycleListener)l).postCreate(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.CREATE));
			}
			break; 
			case PRE_DELETE: if (DeleteLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((DeleteLifecycleListener)l).preDelete(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.DELETE));
			}
			break; 
			case POST_DELETE: if (DeleteLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((DeleteLifecycleListener)l).postDelete(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.DELETE));
			}
			break; 
			case PRE_DIRTY: if (DirtyLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((DirtyLifecycleListener)l).preDirty(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.DIRTY));
			}
			break; 
			case POST_DIRTY: if (DirtyLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((DirtyLifecycleListener)l).postDirty(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.DIRTY));
			}
			break; 
			case LOAD: if (LoadLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((LoadLifecycleListener)l).postLoad(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.LOAD));
			}
			break; 
			case PRE_STORE: if (StoreLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((StoreLifecycleListener)l).preStore(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.STORE));
			}
			break; 
			case POST_STORE: if (StoreLifecycleListener.class.isAssignableFrom(l.getClass())) {
				((StoreLifecycleListener)l).postStore(
						new InstanceLifecycleEvent(src, InstanceLifecycleEvent.STORE));
			}
			break;
			default:
				throw new IllegalArgumentException(event.name());
			}
		}
	}
}
