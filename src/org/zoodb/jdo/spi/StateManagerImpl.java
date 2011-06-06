package org.zoodb.jdo.spi;

import javax.jdo.PersistenceManager;
import javax.jdo.spi.Detachable;
import javax.jdo.spi.PersistenceCapable;
import javax.jdo.spi.StateManager;

import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.client.session.ClientSessionCache;

public class StateManagerImpl implements StateManager {

	//TZ:
	public final Session _session;
	public ClientSessionCache _cache;
	
	static final int JDO_PC_INVALID = 0;
	static final int JDO_PC_DIRTY = 1;
	static final int JDO_PC_NEW = 2;
	static final int JDO_PC_DELETED = 4;
	static final int JDO_PC_DETACHED = 8;
	static final int JDO_PC_PERSISTENT = 16;
	static final int JDO_PC_TRANSACTIONAL = 32;
	
	
	//JDO 2.0
	
	
	public StateManagerImpl(Session session) {
		_session = session;
		//_cache = _session.getClientCache();
	}

	/**
	 * Only to be called by Session!
	 * @param cache
	 */
	public void setCache(ClientSessionCache cache) {
		_cache = cache;
	}

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
		return ((PersistenceCapableImpl)arg0).jdoZooGetOid();
	}

	@Override
	public PersistenceManager getPersistenceManager(PersistenceCapable arg0) {
		return _session.getPersistenceManager();
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
		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
		return co.isDeleted();
		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_DELETED);
	}

	@Override
	public boolean isDirty(PersistenceCapable arg0) {
		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
		return co.isDirty();
		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_DIRTY);
	}

	@Override
	public boolean isLoaded(PersistenceCapable arg0, int arg1) {
		//TODO correct?
		Long oid = (Long) ((PersistenceCapableImpl) arg0).jdoZooGetOid();
		CachedObject co = _cache.findCoByOID(oid);
//		if (co == null || co.isStateHollow()) {
//			
//		}
		return !co.isStateHollow();
	}

	@Override
	public boolean isNew(PersistenceCapable arg0) {
		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
		return co.isNew();
		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_NEW);
	}

	@Override
	public boolean isPersistent(PersistenceCapable arg0) {
		//CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
		//return co.isPDeleted();
		//TODO check for isDeleted? isDetached?
		return true; //If it has a STateManager, it must be persistent. //TODO not true!!! See isLoaded()
		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_PERSISTENT);
	}

	@Override
	public boolean isTransactional(PersistenceCapable arg0) {
		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
		return co.isTransactional();
		//return ((PersistenceCapableImpl)arg0).jdoZooFlagIsSet(JDO_PC_TRANSACTIONAL);
	}

	@Override
	public void makeDirty(PersistenceCapable arg0, String arg1) {
		CachedObject co = _cache.findCO((PersistenceCapableImpl) arg0);
		// TODO Fix, update/use field information
		co.markDirty();
		//((PersistenceCapableImpl)arg0).jdoZooSetFlag(JDO_PC_DIRTY);
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
