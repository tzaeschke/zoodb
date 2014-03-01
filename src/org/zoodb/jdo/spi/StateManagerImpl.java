/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.spi;

import javax.jdo.PersistenceManager;
import javax.jdo.spi.Detachable;
import javax.jdo.spi.PersistenceCapable;
import javax.jdo.spi.StateManager;

import org.zoodb.api.impl.ZooPCImpl;
import org.zoodb.internal.Session;

public class StateManagerImpl implements StateManager {

	public static final StateManager STATEMANAGER = new StateManagerImpl();
	
//	private static final byte PS_PERSISTENT = 1;
//	private static final byte PS_TRANSACTIONAL = 2;
//	private static final byte PS_DIRTY = 4;
//	private static final byte PS_NEW = 8;
//	private static final byte PS_DELETED = 16;
//	private static final byte PS_DETACHED = 32;

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
//	
//	private final ClassNodeSessionBundle bundle;

//	public static class CachedSchema extends StateManagerImpl {
//		public final ZooClassDef schema;
//		public CachedSchema(ZooClassDef schema, ObjectState state) {
//			super(schema.getBundle());
//			this.schema = schema;
//		}
//		/**
//		 * 
//		 * @return The ZooClassDef associated with this cached schema.
//		 */
//		public ZooClassDef getSchema() {
//			return schema;
//		}
//	}
//	
//	public StateManagerImpl() {
//		this.bundle = null;
//	}
//	
//	public StateManagerImpl(ClassNodeSessionBundle bundle) {
//		this.bundle = bundle;
//	}
//	
//	public final Node getNode() {
//		return bundle.getNode();
//	}
	
	// *************************************************************************
	// StateManagerImpl
	// *************************************************************************
		
	//JDO 2.0

	@Override
	public boolean getBooleanField(PersistenceCapable arg0, int arg1,
			boolean arg2) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByteField(PersistenceCapable arg0, int arg1, byte arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public char getCharField(PersistenceCapable arg0, int arg1, char arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDoubleField(PersistenceCapable arg0, int arg1, double arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloatField(PersistenceCapable arg0, int arg1, float arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getIntField(PersistenceCapable arg0, int arg1, int arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLongField(PersistenceCapable arg0, int arg1, long arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object getObjectField(PersistenceCapable arg0, int arg1, Object arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObjectId(PersistenceCapable arg0) {
		long oid = ((ZooPCImpl)arg0).jdoZooGetOid();
		return oid == Session.OID_NOT_ASSIGNED ? null : oid;
		//TODO optimize
		//return ((PersistenceCapableImpl)arg0).jdoZooGetOid();
	}

	public long getObjectId(ZooPCImpl arg0) {
		return arg0.jdoZooGetOid();
		//TODO optimize and use
		//return arg0.jdoZooGetOid();
	}

//	public void setObjectId(PersistenceCapable arg0, long oid) {
//		((PersistenceCapableImpl)arg0).jdoZooSetOid(oid);
//		this.oid = oid;
//	}

	@Override
	public PersistenceManager getPersistenceManager(PersistenceCapable arg0) {
		ZooPCImpl zpc = (ZooPCImpl) arg0;
		return (PersistenceManager) zpc.jdoZooGetContext().getSession().getExternalSession();
	}

	@Override
	public short getShortField(PersistenceCapable arg0, int arg1, short arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getStringField(PersistenceCapable arg0, int arg1, String arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getTransactionalObjectId(PersistenceCapable arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public Object getVersion(PersistenceCapable arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
	}

	@Override
	public boolean isDeleted(PersistenceCapable arg0) {
		return ((ZooPCImpl)arg0).jdoZooIsDeleted();
	}

	@Override
	public boolean isDirty(PersistenceCapable arg0) {
		return ((ZooPCImpl)arg0).jdoZooIsDirty();
//		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
//		return co.isDirty();
//		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_DIRTY);
	}

	@Override
	public boolean isLoaded(PersistenceCapable arg0, int arg1) {
		return !((ZooPCImpl)arg0).jdoZooIsStateHollow();
//		//TODO correct?
//		long oid = ((PersistenceCapableImpl) arg0).jdoZooGetOid();
//		CachedObject co = _cache.findCoByOID(oid);
////		if (co == null || co.isStateHollow()) {
////			
////		}
//		return !co.isStateHollow();
	}

	@Override
	public boolean isNew(PersistenceCapable arg0) {
		return ((ZooPCImpl)arg0).jdoZooIsNew();
//		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
//		return co.isNew();
//		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_NEW);
	}

	@Override
	public boolean isPersistent(PersistenceCapable arg0) {
		//CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
		//return co.isPDeleted();
		//TODO check for isDeleted? isDetached?
		return ((ZooPCImpl)arg0).jdoZooIsPersistent();
		//return true; //If it has a STateManager, it must be persistent. //TODO not true!!! See isLoaded()
		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_PERSISTENT);
	}

	@Override
	public boolean isTransactional(PersistenceCapable arg0) {
		return ((ZooPCImpl)arg0).jdoZooIsTransactional();
//		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
//		return co.isTransactional();
//		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_TRANSACTIONAL);
	}

	@Override
	public void makeDirty(PersistenceCapable arg0, String arg1) {
		((ZooPCImpl)arg0).jdoZooMarkDirty();
//		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
//		// TODO Fix, update/use field information
//		co.markDirty();
//		//((PersistenceCapableImpl)arg0).jdoZooSetFlag(JDO_PC_DIRTY);
	}

	@Override
	public void preSerialize(PersistenceCapable arg0) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
		//return null;
		
	}

	@Override
	public void providedBooleanField(PersistenceCapable arg0, int arg1,
			boolean arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedByteField(PersistenceCapable arg0, int arg1, byte arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedCharField(PersistenceCapable arg0, int arg1, char arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedDoubleField(PersistenceCapable arg0, int arg1,
			double arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedFloatField(PersistenceCapable arg0, int arg1, float arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedIntField(PersistenceCapable arg0, int arg1, int arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedLongField(PersistenceCapable arg0, int arg1, long arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedObjectField(PersistenceCapable arg0, int arg1,
			Object arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedShortField(PersistenceCapable arg0, int arg1, short arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void providedStringField(PersistenceCapable arg0, int arg1,
			String arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean replacingBooleanField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte replacingByteField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public char replacingCharField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object[] replacingDetachedState(Detachable arg0, Object[] arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double replacingDoubleField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte replacingFlags(PersistenceCapable arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float replacingFloatField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int replacingIntField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long replacingLongField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Object replacingObjectField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short replacingShortField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public StateManager replacingStateManager(PersistenceCapable arg0,
			StateManager arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String replacingStringField(PersistenceCapable arg0, int arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setBooleanField(PersistenceCapable arg0, int arg1,
			boolean arg2, boolean arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setByteField(PersistenceCapable arg0, int arg1, byte arg2,
			byte arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCharField(PersistenceCapable arg0, int arg1, char arg2,
			char arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDoubleField(PersistenceCapable arg0, int arg1, double arg2,
			double arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setFloatField(PersistenceCapable arg0, int arg1, float arg2,
			float arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setIntField(PersistenceCapable arg0, int arg1, int arg2,
			int arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLongField(PersistenceCapable arg0, int arg1, long arg2,
			long arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setObjectField(PersistenceCapable arg0, int arg1, Object arg2,
			Object arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setShortField(PersistenceCapable arg0, int arg1, short arg2,
			short arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setStringField(PersistenceCapable arg0, int arg1, String arg2,
			String arg3) {
		// TODO Auto-generated method stub
		
	}
}

