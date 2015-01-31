/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.api.impl;

import javax.jdo.ObjectState;
import javax.jdo.listener.ClearCallback;

import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.client.PCContext;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.Pair;
import org.zoodb.internal.util.Util;
import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.jdo.spi.StateManagerImpl;

/**
 * This is the common super class of all persistent classes.
 * It is separate from PersistenceCapabaleImpl to allow easier separation
 * from JDO. For example, PersistenceCapableImpl implements the
 * PersistenceCapable interface.
 * 
 * @author Tilmann Zaeschke
 */
public abstract class ZooPC {

	private static final byte PS_PERSISTENT = 1;
	private static final byte PS_TRANSACTIONAL = 2;
	private static final byte PS_DIRTY = 4;
	private static final byte PS_NEW = 8;
	private static final byte PS_DELETED = 16;
	private static final byte PS_DETACHED = 32;

//	public enum STATE {
//		TRANSIENT, //optional JDO 2.2
//		//TRANSIENT_CLEAN, //optional JDO 2.2
//		//TRANSIENT_DIRTY, //optional JDO 2.2
//		PERSISTENT_NEW,
//		//PERSISTENT_NON_TRANS, //optional JDO 2.2
//		//PERSISTENT_NON_TRANS_DIRTY, //optional JDO 2.2
//		PERSISTENT_CLEAN,
//		PERSISTENT_DIRTY,
//		HOLLOW,
//		PERSISTENT_DELETED,
//		PERSISTENT_NEW_DELETED,
//		DETACHED_CLEAN,
//		DETACHED_DIRTY
//		;
//	}
	
	
	//store only byte i.o. reference!
	//TODO store only one of the following?
	private transient ObjectState status;
	private transient byte stateFlags;
	
	private transient PCContext context;
	
	//All data except Strings is stored in the long[]
	//Storing the string is only necessary because the the magic number of a string is not
	//as unique as the string itself. So this is only required for collisions in unique
	//string indexes. See Issue #55 in Test_091.
	private transient Pair<long[], Object[]> prevValues = null;
	
	private transient long txTimestamp = Session.TIMESTAMP_NOT_ASSIGNED;
	
	public final boolean jdoZooIsDirty() {
		return (stateFlags & PS_DIRTY) != 0;
	}
	public final boolean jdoZooIsNew() {
		return (stateFlags & PS_NEW) != 0;
	}
	public final boolean jdoZooIsDeleted() {
		return (stateFlags & PS_DELETED) != 0;
	}
	public final boolean jdoZooIsDetached() {
		return (stateFlags & PS_DETACHED) != 0;
	}
	public final boolean jdoZooIsTransactional() {
		return (stateFlags & PS_TRANSACTIONAL) != 0;
	}
	public final boolean jdoZooIsPersistent() {
		return (stateFlags & PS_PERSISTENT) != 0;
	}
	public final Node jdoZooGetNode() {
		return context.getNode();
	}
	//not to be used from outside
	private final void setPersNew() {
		status = ObjectState.PERSISTENT_NEW;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW;
		context.getSession().internalGetCache().notifyDirty(this);
	}
	private final void setPersClean() {
		status = ObjectState.PERSISTENT_CLEAN;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL;
	}
	private final void setPersDirty() {
		status = ObjectState.PERSISTENT_DIRTY;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY;
		context.getSession().internalGetCache().notifyDirty(this);
	}
	private final void setHollow() {
		status = ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL;
		stateFlags = PS_PERSISTENT;
	}
	private final void setPersDeleted() {
		status = ObjectState.PERSISTENT_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_DELETED;
		context.getSession().internalGetCache().notifyDelete(this);
	}
	private final void setPersNewDeleted() {
		status = ObjectState.PERSISTENT_NEW_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW | PS_DELETED;
		context.getSession().internalGetCache().notifyDelete(this);
	}
	private final void setDetachedClean() {
		status = ObjectState.DETACHED_CLEAN;
		stateFlags = PS_DETACHED;
	}
	private final void setDetachedDirty() {
		status = ObjectState.DETACHED_DIRTY;
		stateFlags = PS_DETACHED | PS_DIRTY;
	}
	private final void setTransient() {
		status = ObjectState.TRANSIENT; //TODO other transient states?
		stateFlags = 0;
		jdoZooOid = Session.OID_NOT_ASSIGNED;
	}
	public final void jdoZooMarkClean() {
		//TODO is that all?
		setPersClean();
		prevValues = null;
	}
//	public final void jdoZooMarkNew() {
//		ObjectState statusO = status;
//		if (statusO == ObjectState.TRANSIENT) {
//			setPersNew();
//		} else if (statusO == ObjectState.PERSISTENT_NEW) {
//			//ignore
//		} else { 
//		throw new IllegalStateException("Illegal state transition: " + status 
//				+ " -> Persistent New: " + Util.oidToString(jdoZooOid));
//		}
//	}
	public final void jdoZooMarkDirty() {
		jdoZooGetContext().getSession().internalGetCache().flagOGTraversalRequired();
		switch (status) {
		case DETACHED_DIRTY:
			//is already dirty
			return;
		case DETACHED_CLEAN:
			context.notifyEvent(this, ZooInstanceEvent.PRE_DIRTY);
			setDetachedDirty();
			getPrevValues();
			break;
		case PERSISTENT_NEW:
		case PERSISTENT_DIRTY:
			//is already dirty
			//status = ObjectState.PERSISTENT_DIRTY;
			return;
		case PERSISTENT_CLEAN:
			context.notifyEvent(this, ZooInstanceEvent.PRE_DIRTY);
			setPersDirty();
			getPrevValues();
			break;
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL:
			context.notifyEvent(this, ZooInstanceEvent.PRE_DIRTY);
			//refresh first, then make dirty
			if (getClass() == GenericObject.class) {
				((GenericObject)this).activateRead();
			} else {
				zooActivateRead();
			}
			jdoZooMarkDirty();
			break;
		default:
			throw new IllegalStateException("Illegal state transition: " + status + "->Dirty: " + 
					Util.oidToString(jdoZooOid));
		}
		context.notifyEvent(this, ZooInstanceEvent.POST_DIRTY);
	}
	
	public final void jdoZooMarkDeleted() {
		switch (status) {
		case PERSISTENT_CLEAN:
		case PERSISTENT_DIRTY:
			setPersDeleted(); break;
		case PERSISTENT_NEW:
			setPersNewDeleted(); break;
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL:
			//make a backup! This is a bit of a hack:
			//When deleting an object, we have to remove it from the attr-indexes.
			//However, during removal, the object is marked as PERSISTENT_DELETED,
			//independent of whether it was previously hollow or not. 
			//If it is hollow, we have to refresh() it, otherwise not.
			//So we do the refresh here, where we still know whether it was hollow.
			if (context.getIndexer().isIndexed()) {
				//refresh + createBackup
				context.getNode().refreshObject(this);
			}
			setPersDeleted(); break;
		case PERSISTENT_DELETED:
		case PERSISTENT_NEW_DELETED: 
			throw DBLogger.newUser("The object has already been deleted: " + 
					Util.oidToString(jdoZooOid));
		default: 
			throw new IllegalStateException("Illegal state transition(" + 
					Util.oidToString(jdoZooGetOid()) + "): " + status + "->Deleted");
		}
	}
	
	public final void jdoZooMarkDetached() {
		switch (status) {
		case DETACHED_CLEAN:
		case DETACHED_DIRTY:
			throw new IllegalStateException("Object is already detached");
		default:
			setDetachedClean();
		}
	}
	
	public final void jdoZooMarkHollow() {
		//TODO is that all?
		setHollow();
		prevValues = null;
	}

	public final void jdoZooMarkTransient() {
		switch (status) {
		case TRANSIENT: 
			//nothing to do 
			break;
		case PERSISTENT_CLEAN:
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL: 
			setTransient(); break;
		case PERSISTENT_NEW :
			setTransient(); break;
		case PERSISTENT_DIRTY:
			throw DBLogger.newUser("The object is dirty.");
		case PERSISTENT_DELETED: 
			throw DBLogger.newUser("The object has already been deleted: " + 
					Util.oidToString(jdoZooOid));
		case PERSISTENT_NEW_DELETED :
			setTransient(); break;
		default:
			throw new IllegalStateException("Illegal state transition: " + status + "->Deleted");
		}
	}

	public final boolean jdoZooIsStateHollow() {
		return status == ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL;
	}

	public final PCContext jdoZooGetContext() {
		return context;
	}

	public final ZooClassDef jdoZooGetClassDef() {
		return context.getClassDef();
	}

	public final void jdoZooEvict() {
		if (this instanceof ClearCallback) {
			((ClearCallback)this).jdoPreClear();
		}
		context.notifyEvent(this, ZooInstanceEvent.PRE_CLEAR);
		context.getEvictor().evict(this);
		jdoZooMarkHollow();
		context.notifyEvent(this, ZooInstanceEvent.POST_CLEAR);
	}

	public final boolean jdoZooHasState(ObjectState state) {
		return this.status == state;
	}

	public final void jdoZooInit(ObjectState state, PCContext bundle, long oid) {
		this.context = bundle;
		jdoZooSetOid(oid);
		this.status = state;
		switch (state) {
		case PERSISTENT_NEW: { 
			setPersNew();
			jdoZooSetTimestamp(bundle.getSession().getTransactionId());
			jdoZooGetContext().notifyEvent(this, ZooInstanceEvent.CREATE);
			break;
		}
		case PERSISTENT_CLEAN: { 
			setPersClean();
			break;
		}
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL: { 
			setHollow();
			break;
		}
		default:
			throw new UnsupportedOperationException("" + state);
		}
		if (this instanceof PersistenceCapableImpl) {
			((PersistenceCapableImpl)this).jdoNewInstance(StateManagerImpl.STATEMANAGER);
		}
	}
	
	
	private final void getPrevValues() {
		if (prevValues != null) {
			throw new IllegalStateException();
		}
		prevValues = context.getIndexer().getBackup(this);
	}
	
	public Pair<long[], Object[]> jdoZooGetBackup() {
		return prevValues;
	}

	
	//Specific to ZooDB
	
	/**
	 * This method ensures that the specified object is in the cache.
	 * 
	 * It should be called in the beginning of every method that reads persistent fields.
	 * 
	 * For generated calls, we should not forget private method, because they can be called
	 * from other instances.
	 */
	public final void zooActivateRead() {
		switch (status) {
		case DETACHED_CLEAN:
			//nothing to do
			return;
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL:
			if (jdoZooGetContext().getSession().isClosed()) {
				throw DBLogger.newUser("The PersistenceManager of this object is not open.");
			}
			if (!jdoZooGetContext().getSession().isActive()) {
				throw DBLogger.newUser("The PersistenceManager of this object is not active " +
						"(-> use begin()).");
			}
			jdoZooGetNode().refreshObject(this);
			return;
		case PERSISTENT_DELETED:
		case PERSISTENT_NEW_DELETED:
			throw DBLogger.newUser("The object has been deleted.");
		case PERSISTENT_NEW:
		case PERSISTENT_CLEAN:
		case PERSISTENT_DIRTY:
			//nothing to do
			return;
		case TRANSIENT:
		case TRANSIENT_CLEAN:
		case TRANSIENT_DIRTY:
			//not persistent yet
			return;

		default:
			throw new IllegalStateException("" + status);
		}
	}
	
	/**
	 * This method ensures that the specified object is in the cache and then flags it as dirty.
	 * It includes a call to zooActivateRead().
	 * 
	 * It should be called in the beginning of every method that writes persistent fields.
	 * 
	 * For generated calls, we should not forget private method, because they can be called
	 * from other instances.
	 */
	public final void zooActivateWrite() {
		switch (status) {
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL:
			//pc.jdoStateManager.getPersistenceManager(pc).refresh(pc);
			if (jdoZooGetContext().getSession().isClosed()) {
				throw DBLogger.newUser("The PersitenceManager of this object is not open.");
			}
			if (!jdoZooGetContext().getSession().isActive()) {
				throw DBLogger.newUser("The PersitenceManager of this object is not active " +
						"(-> use begin()).");
			}
			jdoZooGetNode().refreshObject(this);
			break;
		case PERSISTENT_DELETED:
		case PERSISTENT_NEW_DELETED:
			throw DBLogger.newUser("The object has been deleted.");
		case TRANSIENT:
		case TRANSIENT_CLEAN:
		case TRANSIENT_DIRTY:
			//not persistent yet
			return;
		default:
		}
		jdoZooMarkDirty();
	}
	
	public final void zooActivateWrite(String field) {
		//Here we cannot skip loading the field to be loaded, because it may be read beforehand
		zooActivateWrite();
	}
	
	//	private long jdoZooFlags = 0;
    //TODO instead use some fixed value like INVALID_OID
	private transient long jdoZooOid = Session.OID_NOT_ASSIGNED;
	
//	void jdoZooSetFlag(long flag) {
//		jdoZooFlags |= flag;
//	}
//	
//	void jdoZooUnsetFlag(long flag) {
//		jdoZooFlags &= ~flag;
//	}
//	
//	boolean jdoZooFlagIsSet(long flag) {
//		return (jdoZooFlags & flag) > 0;
//	}
//	public void jdoZooSetDirty() { jdoZooSetFlag(StateManagerImpl.JDO_PC_DIRTY); }
//	public void jdoZooSetNew() { jdoZooSetFlag(StateManagerImpl.JDO_PC_NEW); }
//	public void jdoZooSetDeleted() { jdoZooSetFlag(StateManagerImpl.JDO_PC_DELETED); }
//	public void jdoZooSetPersistent() { jdoZooSetFlag(StateManagerImpl.JDO_PC_PERSISTENT); }
//	public void jdoZooSetDirtyNewFalse() { 
//		jdoZooUnsetFlag(StateManagerImpl.JDO_PC_DIRTY | StateManagerImpl.JDO_PC_NEW); 
//	}
	public final void jdoZooSetOid(long oid) { jdoZooOid = oid;}
	public final long jdoZooGetOid() { return jdoZooOid; }
	
	
	//TODO
	public ZooPC() {
		super();
		setTransient();
		//jdoStateManager = StateManagerImpl.SINGLE;
	}
	
	@Override
	public String toString() {
		return super.toString() + " oid=" + Util.oidToString(jdoZooOid) + " state=" + status; 
	}
	
	public void jdoZooSetTimestamp(long ts) {
		txTimestamp = ts;
	}
	
	public long jdoZooGetTimestamp() {
		return txTimestamp;
	}
} // end class definition

