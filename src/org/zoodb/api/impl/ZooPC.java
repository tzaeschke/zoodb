/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.api.impl;

import java.io.Serializable;

import javax.jdo.ObjectState;
import javax.jdo.listener.ClearCallback;

import org.zoodb.api.ZooInstanceEvent;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.Node;
import org.zoodb.internal.Session;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.client.PCContext;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.DBTracer;
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
public abstract class ZooPC implements Serializable {

    private static final long serialVersionUID = 1L;
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
	
	
	//	private long jdoZooFlags = 0;
	//The following are NOT transient because they should survive (de-)serialization
	//by third parties.
	private long jdoZooOid = Session.OID_NOT_ASSIGNED;
	private long txTimestamp = Session.TIMESTAMP_NOT_ASSIGNED;
	
	public final boolean jdoZooIsDirty() {
		return (getStatusFlags() & PS_DIRTY) != 0;
	}
	public final boolean jdoZooIsNew() {
		return (getStatusFlags() & PS_NEW) != 0;
	}
	public final boolean jdoZooIsDeleted() {
		return (getStatusFlags() & PS_DELETED) != 0;
	}
	public final boolean jdoZooIsDetached() {
		return (getStatusFlags() & PS_DETACHED) != 0;
	}
	public final boolean jdoZooIsTransactional() {
		return (getStatusFlags() & PS_TRANSACTIONAL) != 0;
	}
	public final boolean jdoZooIsPersistent() {
		return (getStatusFlags() & PS_PERSISTENT) != 0;
	}
	public final Node jdoZooGetNode() {
		return context.getNode();
	}
	
	private ObjectState getStatus() {
		recoverFromSerialization();
		return status;
	}
	
	private byte getStatusFlags() {
		recoverFromSerialization();
		return stateFlags;
	}
	
	private void recoverFromSerialization() {
		//This is required in case the Object had been serialized/deserialized,
		//which sets the status to null. Spec say it should become DETACHED.
		//Implementation or Serializable interface is not suggested by spec.
		if (status == null) {
			if (jdoZooOid <= 0) {
				setTransient();
			} else {
				setDetachedClean();
			}
		}
	}
	
	//not to be used from outside
	private void setPersNew() {
		status = ObjectState.PERSISTENT_NEW;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW;
		context.getSession().internalGetCache().notifyDirty(this);
	}
	private void setPersClean() {
		status = ObjectState.PERSISTENT_CLEAN;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL;
	}
	private void setPersDirty() {
		status = ObjectState.PERSISTENT_DIRTY;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY;
		context.getSession().internalGetCache().notifyDirty(this);
	}
	private void setHollow() {
		status = ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL;
		stateFlags = PS_PERSISTENT;
	}
	private void setPersDeleted() {
		status = ObjectState.PERSISTENT_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_DELETED;
		context.getSession().internalGetCache().notifyDelete(this);
	}
	private void setPersNewDeleted() {
		status = ObjectState.PERSISTENT_NEW_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW | PS_DELETED;
		context.getSession().internalGetCache().notifyDelete(this);
	}
	private void setDetachedClean() {
		status = ObjectState.DETACHED_CLEAN;
		stateFlags = PS_DETACHED;
	}
	private void setDetachedDirty() {
		status = ObjectState.DETACHED_DIRTY;
		stateFlags = PS_DETACHED | PS_DIRTY;
	}
	private void setTransient() {
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
		switch (getStatus()) {
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
			throw new IllegalStateException("Illegal state transition: " + getStatus() + 
					"->Dirty: " + Util.oidToString(jdoZooOid));
		}
		context.notifyEvent(this, ZooInstanceEvent.POST_DIRTY);
	}
	
	public final void jdoZooMarkDeleted() {
		switch (getStatus()) {
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
					Util.oidToString(jdoZooGetOid()) + "): " + getStatus() + "->Deleted");
		}
	}
	
	public final void jdoZooMarkDetached() {
		switch (getStatus()) {
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
		switch (getStatus()) {
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
			throw new IllegalStateException("Illegal state transition: " + 
					getStatus() + "->Deleted");
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
		case PERSISTENT_DIRTY: {
			//This should only be called from on-demand evolution in GenericObjects
			if (!(this instanceof GenericObject) && !jdoZooIsDetached()) {
				throw new UnsupportedOperationException("" + state);
			}
			setPersDirty();
			break;
		}
		default:
			throw new UnsupportedOperationException("" + state);
		}
		if (this instanceof PersistenceCapableImpl) {
			((PersistenceCapableImpl)this).jdoNewInstance(StateManagerImpl.STATEMANAGER);
		}
	}
	
	
	private void getPrevValues() {
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
		if (DBTracer.TRACE) DBTracer.logCall(this);
		switch (getStatus()) {
		case DETACHED_CLEAN:
			//nothing to do
			return;
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL:
			try {
				Session session = context.getSession();
				session.lock();
				if (session.isClosed()) {
					throw DBLogger.newUser("The PersistenceManager of this object is not open.");
				}
				if (!session.isActive() && !session.getConfig().getNonTransactionalRead()) {
					throw DBLogger.newUser("The PersistenceManager of this object is not active " +
							"(-> use begin()).");
				}
				jdoZooGetNode().refreshObject(this);
			} finally {
				context.getSession().unlock();
			}
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
			throw new IllegalStateException("" + getStatus());
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
		if (DBTracer.TRACE) DBTracer.logCall(this);
		switch (getStatus()) {
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL:
			try {
				context.getSession().lock();
				checkActiveForWrite();
				jdoZooGetNode().refreshObject(this);
				jdoZooMarkDirty();
				return;
			} finally {
				context.getSession().unlock();
			}
		case PERSISTENT_DELETED:
		case PERSISTENT_NEW_DELETED:
			throw DBLogger.newUser("The object has been deleted.");
		case TRANSIENT:
		case TRANSIENT_CLEAN:
		case TRANSIENT_DIRTY:
			//not persistent yet
			return;
		case PERSISTENT_DIRTY:
		case PERSISTENT_NEW:
		case DETACHED_DIRTY:
			//nothing to do
			return;
		case PERSISTENT_CLEAN:
			try {
				context.getSession().lock();
				checkActiveForWrite();
				jdoZooMarkDirty();
				return;
			} finally {
				context.getSession().unlock();
			}
		case DETACHED_CLEAN:
			try {
				context.getSession().lock();
				jdoZooMarkDirty();
				return;
			} finally {
				context.getSession().unlock();
			}
		default:
		}
		throw new UnsupportedOperationException(getStatus().toString());
	}

	private void checkActiveForWrite() {
		if (jdoZooGetContext().getSession().isClosed()) {
			throw DBLogger.newUser("The PersitenceManager of this object is not open.");
		}
		if (!jdoZooGetContext().getSession().isActive()) {
			throw DBLogger.newUser("The PersitenceManager of this object is not active " +
					"(-> use begin()).");
		}
	}
	
	public final void zooActivateWrite(String field) {
		//Here we cannot skip loading the field to be loaded, because it may be read beforehand
		zooActivateWrite();
	}
	
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
		return super.toString() + " oid=" + Util.oidToString(jdoZooOid) + 
				" state=" + getStatus(); 
	}
	
	public void jdoZooSetTimestamp(long ts) {
		txTimestamp = ts;
	}
	
	public long jdoZooGetTimestamp() {
		return txTimestamp;
	}
	
//	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
//		out.writeLong(jdoZooOid);
//		out.writeObject(status);
//		out.writeByte(stateFlags);
////		throw new UnsupportedOperationException();
//	}
//
//	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
//		jdoZooOid = in.readLong();
//		status = (ObjectState) in.readObject();
//		stateFlags = in.readByte();
////		throw new UnsupportedOperationException();
//		if (status != ObjectState.TRANSIENT) {
//			setDetachedClean();
//		}
//	}
//	private void readObjectNoData() throws ObjectStreamException {
//		throw new UnsupportedOperationException();
//	}
	
}

