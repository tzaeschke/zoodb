package org.zoodb.jdo.internal.client;

import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Util;

public class CachedPCI {
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
	private transient byte status;
	private transient byte stateFlags;
	
	private transient long oid;
	private transient final ClassNodeSessionBundle bundle;

	public CachedPCI(ObjectState state, 
			ClassNodeSessionBundle bundle) {
		this.bundle = bundle;
		status = (byte)state.ordinal();
		switch (state) {
		case PERSISTENT_NEW: { 
			setPersNew();
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
	}
	
	public boolean isDirty() {
		return (stateFlags & PS_DIRTY) != 0;
	}
	public boolean isNew() {
		return (stateFlags & PS_NEW) != 0;
	}
	public boolean isDeleted() {
		return (stateFlags & PS_DELETED) != 0;
	}
	public boolean isTransactional() {
		return (stateFlags & PS_TRANSACTIONAL) != 0;
	}
	public boolean isPersistent() {
		return (stateFlags & PS_PERSISTENT) != 0;
	}
	public final long getOID() {
		return oid;
	}
	public final Node getNode() {
		return bundle.getNode();
	}
	//not to be used from outside
	private void setPersNew() {
		status = (byte)ObjectState.PERSISTENT_NEW.ordinal();
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW;
	}
	private void setPersClean() {
		status = (byte)ObjectState.PERSISTENT_CLEAN.ordinal();
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL;
	}
	private void setPersDirty() {
		status = (byte)ObjectState.PERSISTENT_DIRTY.ordinal();
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY;
	}
	private void setHollow() {
		status = (byte)ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL.ordinal();
		stateFlags = PS_PERSISTENT;
	}
	private void setPersDeleted() {
		status = (byte)ObjectState.PERSISTENT_DELETED.ordinal();
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_DELETED;
	}
	private void setPersNewDeleted() {
		status = (byte)ObjectState.PERSISTENT_NEW_DELETED.ordinal();
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW | PS_DELETED;
	}
	private void setDetachedClean() {
		status = (byte)ObjectState.DETACHED_CLEAN.ordinal();
		stateFlags = PS_DETACHED;
	}
	private void setDetachedDirty() {
		status = (byte)ObjectState.DETACHED_DIRTY.ordinal();
		stateFlags = PS_DETACHED | PS_DIRTY;
	}
	public void markClean() {
		//TODO is that all?
		setPersClean();
	}
	public void markDirty() {
		ObjectState statusO = ObjectState.values()[status];
		if (statusO == ObjectState.PERSISTENT_CLEAN) {
			setPersDirty();
		} else if (statusO == ObjectState.PERSISTENT_NEW) {
			//is already dirty
			//status = ObjectState.PERSISTENT_DIRTY;
		} else if (statusO == ObjectState.PERSISTENT_DIRTY) {
			//is already dirty
			//status = ObjectState.PERSISTENT_DIRTY;
		} else {
			throw new IllegalStateException("Illegal state transition: " + status + "->Dirty: " + 
					Util.oidToString(getOID()));
		}
	}
	public void markDeleted() {
		ObjectState statusO = ObjectState.values()[status];
		if (statusO == ObjectState.PERSISTENT_CLEAN ||
				statusO == ObjectState.PERSISTENT_DIRTY) {
			setPersDeleted();
		} else if (statusO == ObjectState.PERSISTENT_NEW) {
			setPersNewDeleted();
		} else if (statusO == ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL) {
			setPersDeleted();
		} else if (statusO == ObjectState.PERSISTENT_DELETED 
				|| statusO == ObjectState.PERSISTENT_NEW_DELETED) {
			throw new JDOUserException("The object has already been deleted: " + 
					Util.oidToString(getOID()));
		} else {
			throw new IllegalStateException("Illegal state transition: " + status + "->Deleted");
		}
	}
	public void markHollow() {
		//TODO is that all?
		setHollow();
	}

	public boolean isStateHollow() {
		return status == ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL.ordinal();
	}

	public PersistenceManager getPM() {
		return bundle.getSession().getPersistenceManager();
	}

}
