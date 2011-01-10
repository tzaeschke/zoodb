/**
 * 
 */
package org.zoodb.jdo.internal.client;

import javax.jdo.ObjectState;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class CachedObject {
	private static final long PS_PERSISTENT = 1;
	private static final long PS_TRANSACTIONAL = 2;
	private static final long PS_DIRTY = 4;
	private static final long PS_NEW = 8;
	private static final long PS_DELETED = 16;
	private static final long PS_DETACHED = 32;

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
	
	private ObjectState status;
	private long stateFlags;
	
	public final long oid = Session.OID_NOT_ASSIGNED;
	public PersistenceCapableImpl obj = null;
	public Node node;

	public static class CachedSchema extends CachedObject {
		public ZooClassDef schema;
		public CachedSchema(ZooClassDef schema, ObjectState state, Node node) {
			super(state);
			this.oid = schema.getOid();
			this.schema = schema;
			this.node = node;
		}
		public ZooClassDef getSchema() {
			return schema;
		}
		public Node getNode() {
			return node;
		}
	}
	public CachedObject(ObjectState state) {
		status = state;
		switch (status) {
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
	public long getOID() {
		return oid;
	}
	public PersistenceCapableImpl getObject() {
		return obj;
	}
	public Node getNode() {
		return node;
	}
	//not to be used from outside
	private void setPersNew() {
		status = ObjectState.PERSISTENT_NEW;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW;
	}
	private void setPersClean() {
		status = ObjectState.PERSISTENT_CLEAN;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL;
	}
	private void setPersDirty() {
		status = ObjectState.PERSISTENT_DIRTY;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY;
	}
	private void setHollow() {
		status = ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL;
		stateFlags = PS_PERSISTENT;
	}
	private void setPersDeleted() {
		status = ObjectState.PERSISTENT_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_DELETED;
	}
	private void setPersNewDeleted() {
		status = ObjectState.PERSISTENT_NEW_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW | PS_DELETED;
	}
	private void setDetachedClean() {
		status = ObjectState.DETACHED_CLEAN;
		stateFlags = PS_DETACHED;
	}
	private void setDetachedDirty() {
		status = ObjectState.DETACHED_DIRTY;
		stateFlags = PS_DETACHED | PS_DIRTY;
	}
	public void markClean() {
		//TODO is that all?
		setPersClean();
	}
	public void markDirty() {
		if (status == ObjectState.PERSISTENT_CLEAN) {
			setPersDirty();
		} else if (status == ObjectState.PERSISTENT_NEW) {
			//is already dirty
			//status = ObjectState.PERSISTENT_DIRTY;
		} else {
			throw new IllegalStateException("Illegal state transition: " + status + "->Dirty");
		}
	}
	public void markDeleted() {
		if (status == ObjectState.PERSISTENT_CLEAN ||
				status == ObjectState.PERSISTENT_DIRTY) {
			setPersDeleted();
		} else if (status == ObjectState.PERSISTENT_NEW) {
			setPersNewDeleted();
		} else if (status == ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL) {
			setPersDeleted();
		} else {
			throw new IllegalStateException("Illegal state transition: " + status + "->Deleted");
		}
	}
	public void markHollow() {
		//TODO is that all?
		setHollow();
	}

	public boolean isStateHollow() {
		return status == ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL;
	}
}

