package org.zoodb.jdo.spi;

import javax.jdo.JDOFatalInternalException;
import javax.jdo.JDOUserException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.identity.IntIdentity;
import javax.jdo.spi.JDOImplHelper;
import javax.jdo.spi.PersistenceCapable;
import javax.jdo.spi.StateManager;

import org.zoodb.jdo.internal.Node;
import org.zoodb.jdo.internal.Session;
import org.zoodb.jdo.internal.Util;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.jdo.internal.client.ClassNodeSessionBundle;

public class PersistenceCapableImpl implements PersistenceCapable {

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
	
	private transient ClassNodeSessionBundle bundle;
	
	public final boolean jdoZooIsDirty() {
		return (stateFlags & PS_DIRTY) != 0;
	}
	public final boolean jdoZooIsNew() {
		return (stateFlags & PS_NEW) != 0;
	}
	public final boolean jdoZooIsDeleted() {
		return (stateFlags & PS_DELETED) != 0;
	}
	public final boolean jdoZooIsTransactional() {
		return (stateFlags & PS_TRANSACTIONAL) != 0;
	}
	public final boolean jdoZooIsPersistent() {
		return (stateFlags & PS_PERSISTENT) != 0;
	}
	public final Node jdoZooGetNode() {
		return bundle.getNode();
	}
	//not to be used from outside
	private final void setPersNew() {
		status = ObjectState.PERSISTENT_NEW;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW;
	}
	private final void setPersClean() {
		status = ObjectState.PERSISTENT_CLEAN;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL;
	}
	private final void setPersDirty() {
		status = ObjectState.PERSISTENT_DIRTY;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY;
	}
	private final void setHollow() {
		status = ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL;
		stateFlags = PS_PERSISTENT;
	}
	private final void setPersDeleted() {
		status = ObjectState.PERSISTENT_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_DELETED;
	}
	private final void setPersNewDeleted() {
		status = ObjectState.PERSISTENT_NEW_DELETED;
		stateFlags = PS_PERSISTENT | PS_TRANSACTIONAL | PS_DIRTY | PS_NEW | PS_DELETED;
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
		ObjectState statusO = status;
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
					Util.oidToString(jdoZooOid));
		}
	}
	public final void jdoZooMarkDeleted() {
		ObjectState statusO = status;
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
					Util.oidToString(jdoZooOid));
		} else {
			throw new IllegalStateException("Illegal state transition: " + status + "->Deleted");
		}
	}
	public final void jdoZooMarkHollow() {
		//TODO is that all?
		setHollow();
	}

	public final void jdoZooMarkTransient() {
		ObjectState statusO = status;
		if (statusO == ObjectState.TRANSIENT) {
			//nothing to do 
		} else if (statusO == ObjectState.PERSISTENT_CLEAN ||
				statusO == ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL) {
			setTransient();
		} else if (statusO == ObjectState.PERSISTENT_NEW) {
			throw new JDOUserException("The object is new.");
		} else if (statusO == ObjectState.PERSISTENT_DIRTY) {
			throw new JDOUserException("The object is dirty.");
		} else if (statusO == ObjectState.PERSISTENT_DELETED 
				|| statusO == ObjectState.PERSISTENT_NEW_DELETED) {
			throw new JDOUserException("The object has already been deleted: " + 
					Util.oidToString(jdoZooOid));
		} else {
			throw new IllegalStateException("Illegal state transition: " + status + "->Deleted");
		}
	}

	public final boolean jdoZooIsStateHollow() {
		return status == ObjectState.HOLLOW_PERSISTENT_NONTRANSACTIONAL;
	}

	public final PersistenceManager jdoZooGetPM() {
		return bundle.getSession().getPersistenceManager();
	}

	protected final ClassNodeSessionBundle jdoZooGetBundle() {
		return bundle;
	}

	public final ZooClassDef jdoZooGetClassDef() {
		return bundle.getClassDef();
	}

	public final boolean jdoZooHasState(ObjectState state) {
		return this.status == state;
	}

	public final void jdoZooInit(ObjectState state, ClassNodeSessionBundle bundle, long oid) {
		this.bundle = bundle;
		jdoZooSetOid(oid);
		this.status = state;
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
		case HOLLOW_PERSISTENT_NONTRANSACTIONAL:
			//pc.jdoStateManager.getPersistenceManager(pc).refresh(pc);
			if (jdoZooGetPM().isClosed()) {
				throw new JDOUserException("The PersitenceManager of this object is not open.");
			}
			if (!jdoZooGetPM().currentTransaction().isActive()) {
				throw new JDOUserException("The PersitenceManager of this object is not active " +
						"(-> use begin()).");
			}
			jdoZooGetNode().refreshObject(this);
			return;
		case PERSISTENT_DELETED:
		case PERSISTENT_NEW_DELETED:
			throw new JDOUserException("The object has been deleted.");
		case TRANSIENT:
		case TRANSIENT_CLEAN:
		case TRANSIENT_DIRTY:
			//not persistent yet
			return;

		default:
			break;
		}
//		//if (pc.jdoZooOid == null || pc.jdoZooOid.equals(Session.OID_NOT_ASSIGNED)) {
//		if (jdoZooOid == Session.OID_NOT_ASSIGNED) {
//			//not persistent yet
//			return;
//		}
//		if (isDeleted()) {
//			throw new JDOUserException("The object has been deleted.");
//		}
//		if (isStateHollow()) {
//			//pc.jdoStateManager.getPersistenceManager(pc).refresh(pc);
//			if (getPM().isClosed()) {
//				throw new JDOUserException("The PersitenceManager of this object is not open.");
//			}
//			getNode().refreshObject(this);
//		}
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
			if (jdoZooGetPM().isClosed()) {
				throw new JDOUserException("The PersitenceManager of this object is not open.");
			}
			if (!jdoZooGetPM().currentTransaction().isActive()) {
				throw new JDOUserException("The PersitenceManager of this object is not active " +
						"(-> use begin()).");
			}
			jdoZooGetNode().refreshObject(this);
			break;
		case PERSISTENT_DELETED:
		case PERSISTENT_NEW_DELETED:
			throw new JDOUserException("The object has been deleted.");
		case TRANSIENT:
		case TRANSIENT_CLEAN:
		case TRANSIENT_DIRTY:
			//not persistent yet
			return;

		default:
			break;
		}
//		zooActivateRead();
//		if (jdoZooOid == Session.OID_NOT_ASSIGNED) {
//			//not persistent yet
//			return;
//		}
		jdoZooMarkDirty();
	}
	
	public final void zooActivateWrite(String field) {
		//Here we can not skip loading the field to be loaded, because it may be read beforehand
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
	public final void jdoZooSetOid(long oid) { jdoZooOid = oid; }
	public final long jdoZooGetOid() { return jdoZooOid; }
	
	
	//TODO
	public PersistenceCapableImpl() {
		super();
		setTransient();
		//jdoStateManager = StateManagerImpl.SINGLE;
	}
	
	
	//FROM JDO 2.2 chapter 23
	//protected transient StateManager jdoStateManager = null;
	private transient StateManager jdoStateManager = null;
	//protected transient byte jdoFlags =
	private transient byte jdoFlags =
		javax.jdo.spi.PersistenceCapable.READ_WRITE_OK;
	// if no superclass, the following:
//	private final static int jdoInheritedFieldCount = 0;
//	/* otherwise,
//	private final static int jdoInheritedFieldCount =
//	<persistence-capable-superclass>.jdoGetManagedFieldCount();
//	 */
//	//	private final static String[] jdoFieldNames = {"boss", "dept", "empid", "name"};
//	//	private final static Class[] jdoFieldTypes = {Employee.class, Department.class,
//	//		int.class, String.class};
//	private final static String[] jdoFieldNames = {};//TODO
//	private final static Class[] jdoFieldTypes = {};//TODO
//	private final static byte[] jdoFieldFlags = {
//		MEDIATE_READ+MEDIATE_WRITE,
//		MEDIATE_READ+MEDIATE_WRITE,
//		MEDIATE_WRITE,
//		CHECK_READ+CHECK_WRITE
//	};
//	// if no PersistenceCapable superclass, the following:
//	private final static Class jdoPersistenceCapableSuperclass = null;
//	/* otherwise,
//	private final static Class jdoPersistenceCapableSuperclass = <pc-super>;
//	private final static long serialVersionUID = 1234567890L;
//	 */
//
//	static {
//		//		javax.jdo.spi.JDOImplHelper.registerClass (
//		//				Employee.class,
//		//				jdoFieldNames,
//		//				jdoFieldTypes,
//		//				jdoFieldFlags,
//		//				jdoPersistenceCapableSuperclass,
//		//				new Employee());
//		//TODO
//	}


	//23.21.3 Generated interrogatives
	@Override
	public final boolean jdoIsPersistent() {
		return jdoStateManager==null?false:
			jdoStateManager.isPersistent(this);
	}
	@Override
	public final boolean jdoIsTransactional(){
		return jdoStateManager==null?false:
			jdoStateManager.isTransactional(this);
	}
	@Override
	public final boolean jdoIsNew(){
		return jdoStateManager==null?false:
			jdoStateManager.isNew(this);
	}
	@Override
	public final boolean jdoIsDirty(){
		return jdoStateManager==null?false:
			jdoStateManager.isDirty(this);
	}
	@Override
	public final boolean jdoIsDeleted(){
		return jdoStateManager==null?false:
			jdoStateManager.isDeleted(this);
	}
	@Override
	public final boolean jdoIsDetached(){
		System.out.println("STUB: PersistenceCapableImpl.jdoIsDetached()"); //TODO
//		return jdoStateManager==null?false:
//			jdoStateManager.isDetached(this);
		return false;
	}
	@Override
	public final void jdoMakeDirty (String fieldName){
		if (jdoStateManager==null) return;
		jdoStateManager.makeDirty(this, fieldName);
	}
	@Override
	public final PersistenceManager jdoGetPersistenceManager(){
		return jdoStateManager==null?null:
			jdoStateManager.getPersistenceManager(this);
	}
	@Override
	public final Object jdoGetObjectId(){
		Object oid = jdoStateManager==null?null:
			jdoStateManager.getObjectId(this);
		return ((Long)Session.OID_NOT_ASSIGNED).equals(oid) ? null : oid; //TODO ?? double checking oid??
	}
	@Override
	public final Object jdoGetTransactionalObjectId(){
		return jdoStateManager==null?null:
			jdoStateManager.getTransactionalObjectId(this);
	}

	
	//23.21.4 Generated jdoReplaceStateManager
	/**
	 * The generated method asks the current StateManager to approve the change or validates the
	 * caller’s authority to set the state.
	 */
	@Override
	public final synchronized void jdoReplaceStateManager
	(javax.jdo.spi.StateManager sm) {
		// throws exception if current sm didn’t request the change
		if (jdoStateManager != null) {
			jdoStateManager = jdoStateManager.replacingStateManager (this, sm);
		} else {
			// the following will throw an exception if not authorized
			JDOImplHelper.checkAuthorizedStateManager(sm);
			jdoStateManager = sm;
			this.jdoFlags = LOAD_REQUIRED;
		}
	}

	
	//23.21.5 Generated jdoReplaceFlags
	@Override
	public final void jdoReplaceFlags () {
		if (jdoStateManager != null) {
			jdoFlags = jdoStateManager.replacingFlags (this);
		}
	}

	
	//23.21.6 Generated jdoNewInstance helpers
	/**
	 * The first generated helper assigns the value of the passed parameter to the jdoStateManager
	 * field of the newly created instance.
	*/
	@Override
	public PersistenceCapable jdoNewInstance(StateManager sm) {
		//--TZ begin
		this.jdoStateManager = sm;
		return null;
		//throw new UnsupportedOperationException("Needs to be generated.");
		//--TZ end
		// if class is abstract, throw new JDOFatalInternalException()
//		Employee pc = new Employee ();
//		pc.jdoStateManager = sm;
//		pc.jdoFlags = LOAD_REQUIRED;
//		return pc;
	} 
	/** 
	 * The second generated helper assigns the value of the passed parameter to the jdoStateManager
	 * field of the newly created instance, and initializes the values of the key fields from the oid
	 * parameter.
	 */
	@Override
	public PersistenceCapable jdoNewInstance(StateManager sm, Object oid) {
		// if class is abstract, throw new JDOFatalInternalException()
        throw new UnsupportedOperationException("Needs to be generated.");
//		Employee pc = new Employee ();
//		pc.jdoStateManager = sm;
//		pc.jdoFlags = LOAD_REQUIRED;
//		// now copy the key fields into the new instance
//		jdoCopyKeyFieldsFromObjectId (oid);
//		return pc;
	}






	//23.21.7 Generated jdoGetManagedFieldCount
	/** 
	 * The generated method returns the number of managed fields in this class plus the number of inherited
	 * managed fields. This method is expected to be executed only during class loading of the subclasses.
	 */	
	//		The implementation for topmost classes in the hierarchy:
	protected static int jdoGetManagedFieldCount () {
        throw new UnsupportedOperationException("Needs to be generated.");
//		return <enhancer-generated constant>;
	}
	//			The implementation for subclasses:
//	protected static int jdoGetManagedFieldCount () {
//		return <pc-superclass>.jdoGetManagedFieldCount() +
//		<enhancer-generated constant>;
//	}

	
	//23.21.8 Generated jdoGetXXX methods (one per persistent field)
//	/**
//	 * The access modifier is the same modifier as the corresponding field definition. Therefore, access to
//	 * the method is controlled by the same policy as for the corresponding field.
//	 */
//	final static String
//	jdoGetname(Employee x) {
//		// this field is in the default fetch group (CHECK_READ)
//		if (x.jdoFlags <= READ_WRITE_OK) {
//			// ok to read
//			return x.name;
//		}
//		// field needs to be fetched from StateManager
//		// this call might result in name being stored in instance
//		StateManager sm = x.jdoStateManager;
//		if (sm != null) {
//			if (sm.isLoaded (x, jdoInheritedFieldCount + 3))
//				return x.name;
//			return sm.getStringField(x, jdoInheritedFieldCount + 3,
//					x.name);
//		} else {
//			return x.name;
//		}
//	}
//	final static com.xyz.hr.Department
//	jdoGetdept(Employee x) {
//		// this field is not in the default fetch group (MEDIATE_READ)
//		StateManager sm = x.jdoStateManager;
//		if (sm != null) {
//			if (sm.isLoaded (x, jdoInheritedFieldCount + 1))
//				return x.dept;
//			return (com.xyz.hr.Department)
//			sm.getObjectField(x,
//					jdoInheritedFieldCount + 1,
//					x.dept);
//		} else {
//			return x.dept;
//		}
//	}


	//23.21.9 Generated jdoSetXXX methods (one per persistent field)
	/**
	 * The access modifier is the same modifier as the corresponding field definition. Therefore, access to
	 * the method is controlled by the same policy as for the corresponding field.
	 */		
//	final static void jdoSetname(Employee x, String newValue) {
//		// this field is in the default fetch group
//		if (x.jdoFlags == READ_WRITE_OK) {
//			// ok to read, write
//			x.name = newValue;
//			return;
//		}
//		StateManager sm = x.jdoStateManager;
//		if (sm != null) {
//			sm.setStringField(x,
//					jdoInheritedFieldCount + 3,
//					x.name,
//					newValue);
//		} else {
//			x.name = newValue;
//		}
//	}
//	final static void jdoSetdept(Employee x, com.xyz.hr.Department newValue) {
//		// this field is not in the default fetch group
//		StateManager sm = x.jdoStateManager;
//		if (sm != null) {
//			sm.setObjectField(x,
//					jdoInheritedFieldCount + 1,
//					x.dept, newValue);
//		} else {
//			x.dept = newValue;
//		}
//	}



	//23.21.10 Generated jdoReplaceField and jdoReplaceFields
	/**
	 * The generated jdoReplaceField retrieves a new value from the StateManager for one specific
	 * field based on field number. This method is called by the StateManager whenever it wants
	 * to update the value of a field in the instance, for example to store values in the instance from the
	 * datastore.
	 * This may be used by the StateManager to clear fields and handle cleanup of the objects currently
	 * referred to by the fields (e.g., embedded objects).
	 */
	@Override
	public void jdoReplaceField (int fieldNumber) {
        throw new UnsupportedOperationException("Needs to be generated.");
//		int relativeField = fieldNumber - jdoInheritedFieldCount;
//		switch (relativeField) {
//		case (0): boss = (Employee)
//		jdoStateManager.replacingObjectField (this,
//				fieldNumber);
//		break;
//		case (1): dept = (Department)
//		jdoStateManager.replacingObjectField (this,
//				fieldNumber);
//		break;
//		case (2): empid =
//			jdoStateManager.replacingIntField (this,
//					fieldNumber);
//		break;
//		case (3): name =
//			jdoStateManager.replacingStringField (this,
//					fieldNumber);
//		break;
//		default:
//			/* if there is a pc superclass, delegate to it
//				if (relativeField < 0) {
//				super.jdoReplaceField (fieldNumber);
//				} else {
//				throw new IllegalArgumentException("fieldNumber");
//				}
//			 */
//			// if there is no pc superclass, throw an exception
//			throw new IllegalArgumentException("fieldNumber");
//		} // switch
	}
	@Override
	public final void jdoReplaceFields (int[] fieldNumbers) {
		for (int i = 0; i < fieldNumbers.length; ++i) {
			int fieldNumber = fieldNumbers[i];
			jdoReplaceField (fieldNumber);
		}
	}


	//23.21.11 Generated jdoProvideField and jdoProvideFields
	/**
	 * The generated jdoProvideField gives the current value of one field to the StateManager.
	 * This method is called by the StateManager whenever it wants to get the value of a field in the
	 * instance, for example to store the field in the datastore.
	 */
	@Override
	public void jdoProvideField (int fieldNumber) {
        throw new UnsupportedOperationException("Needs to be generated.");
//		int relativeField = fieldNumber - jdoInheritedFieldCount;
//		switch (relativeField) {
//		case (0): jdoStateManager.providedObjectField(this,
//				fieldNumber, boss);
//		break;
//		case (1): jdoStateManager.providedObjectField(this,
//				fieldNumber, dept);
//		break;
//		case (2): jdoStateManager.providedIntField(this,
//				fieldNumber, empid);
//		break;
//		case (3): jdoStateManager.providedStringField(this,
//				fieldNumber, name);
//		break;
//		default:
//			/* if there is a pc superclass, delegate to it
//					if (relativeField < 0) {
//					super.jdoProvideField (fieldNumber);
//					} else {
//					throw new IllegalArgumentException("fieldNumber");
//					}
//			 */
//			// if there is no pc superclass, throw an exception
//			throw new IllegalArgumentException("fieldNumber");
//		} // switch
	}
	@Override
	public final void jdoProvideFields (int[] fieldNumbers) {
		for (int i = 0; i < fieldNumbers.length; ++i) {
			int fieldNumber = fieldNumbers[i];
			jdoProvideField (fieldNumber);
		}
	}


	//23.21.12 Generated jdoCopyField and jdoCopyFields methods
	/**
	 * The generated jdoCopyFields copies fields from another instance to this instance. This method
	 * might be used by the StateManager to create before images of instances for rollback, or to restore
	 * instances in case of rollback.
	 * This method delegates to method jdoCopyField to copy values for all fields requested.
	 * To avoid security exposure, jdoCopyFields can be invoked only when both instances are owned
	 * by the same StateManager. Thus, a malicious user cannot use this method to copy fields from a
	 * managed instance to a non-managed instance, or to an instance managed by a malicious StateManager.
	 */
	@Override
	public void jdoCopyFields (Object pc, int[] fieldNumbers){
		// the other instance must be owned by the same StateManager
		// and our StateManager must not be null!
        throw new UnsupportedOperationException("Needs to be generated.");
//		if (((PersistenceCapableImpl)pc).jdoStateManager
//				!= this.jdoStateManager)
//			throw new IllegalArgumentException("this.jdoStateManager !=	other.jdoStateManager");
//		if (this.jdoStateManager == null)
//			throw new IllegalStateException("this.jdoStateManager == null");
//		// throw ClassCastException if other class is the wrong class
//		Employee other = (Employee) pc;
//		for (int i = 0; i < fieldNumbers.length; ++i) {
//			jdoCopyField (other, fieldNumbers[i]);
//		} // for loop
	} // jdoCopyFields
//	protected void jdoCopyField (Employee other, int fieldNumber) {
//		int relativeField = fieldNumber - jdoInheritedFieldCount;
//        throw new UnsupportedOperationException("Needs to be generated.");
////		switch (relativeField) {
////		case (0): this.boss = other.boss;
////		break;
////		case (1): this.dept = other.dept;
////		break;
////		case (2): this.empid = other.empid;
////		break;
////		case (3): this.name = other.name;
////		break;
////		default: // other fields handled in superclass
////			// this class has no superclass, so throw an exception
////			throw new IllegalArgumentException("fieldNumber");
////			/* if it had a superclass, it would handle the field as follows:
////						super.jdoCopyField (other, fieldNumber);
////			 */
////			break;
////		} // switch
//	} // jdoCopyField


	//23.21.13 Generated writeObject method
	/**
	 * If no user-written method writeObject exists, then one will be generated. The generated writeObject
	 * makes sure that all persistent and transactional serializable fields are loaded into the instance,
	 * and then the default output behavior is invoked on the output stream.
	 */
	private void writeObject(java.io.ObjectOutputStream out)
	throws java.io.IOException{
		jdoPreSerialize();
		out.defaultWriteObject ();
	}


	//23.21.14 Generated jdoPreSerialize method
	/**
	 * The generated jdoPreSerialize method makes sure that all persistent and transactional serializable
	 * fields are loaded into the instance by delegating to the corresponding method in StateManager.
	 */
	private final void jdoPreSerialize() {
		if (jdoStateManager != null)
			jdoStateManager.preSerialize(this);
	}


	//23.21.15 Generated jdoNewObjectIdInstance
	/**
	 * The generated methods create and return a new instance of the object id class.
	 */
	@Override
	public Object jdoNewObjectIdInstance() {
        throw new UnsupportedOperationException("Needs to be generated.");
//		return new IntIdentity(Employee.class, empid);
	}
	@Override
	public Object jdoNewObjectIdInstance(Object obj) {
        throw new UnsupportedOperationException("Needs to be generated.");
//		if (obj instanceof String) {
//			return new IntIdentity(Employee.class, (String)str);
//		} else if (obj instanceof Integer) {
//			return new IntIdentity(Employee.class, (Integer)obj);
//		} else if (obj instanceof ObjectIdFieldSupplier) {
//			return new IntIdentity(Employee.class,
//					((ObjectIdFieldSupplier)obj).fetchIntField(2));
//		} else
//			throw new JDOUserException("illegal object id type");
	}


	//23.21.16 Generated jdoCopyKeyFieldsToObjectId
	/** 
	 * The generated methods copy key field values from the PersistenceCapable instance or from
	 * the ObjectIdFieldSupplier.
	 */
	@Override
	public void jdoCopyKeyFieldsToObjectId (ObjectIdFieldSupplier fs, Object oid) {
		throw new JDOFatalInternalException("Object id is immutable");
	}
	@Override
	public void jdoCopyKeyFieldsToObjectId (Object oid) {
		throw new JDOFatalInternalException("Object id is immutable");
	}



	//23.21.17 Generated jdoCopyKeyFieldsFromObjectId
	/**
	 * The generated methods copy key fields from the object id instance to the PersistenceCapable
	 *instance or to the ObjectIdFieldConsumer.
	 */
	@Override
	public void jdoCopyKeyFieldsFromObjectId (ObjectIdFieldConsumer fc, Object oid) {
		fc.storeIntField (2, ((IntIdentity)oid).getKey());
	}

	/**
	 * This method is part of the PersistenceCapable contract. It copies key fields from the object id instance
	 * to the ObjectIdFieldConsumer.
	 */
	protected void jdoCopyKeyFieldsFromObjectId (Object oid) {
        throw new UnsupportedOperationException("Needs to be generated.");
//		empid = ((IntIdentity)oid).getKey());
	}
	//This method is used internally to copy key fields from the object id instance to a newly created
	//PersistenceCapable instance.



	//23.21.18 Generated Detachable methods
	public void jdoReplaceDetachedState() {
        throw new UnsupportedOperationException("Needs to be generated.");
//		jdoDetachedState = sm.replacingDetachedState(this,
//				jdoDetachedState);
	}
	// end JDO 2.2 class definition


	//	private Long _jdoZooObjectId;
	//	private int _jdoZooFlags;
	//	private static final int _JDO_ZOO_FLAG_PERSISTENT = 1;
	//	private static final int _JDO_ZOO_FLAG_DIRTY = 2;
	//	private static final int _JDO_ZOO_FLAG_NEW = 4;
	//	private static final int _JDO_ZOO_FLAG_DELETED = 8;
	//	private static final int _JDO_ZOO_FLAG_TRANSACTIONAL = 16;
	//	private static final int _JDO_ZOO_FLAG_DETACHED = 32;



	@Override
	public Object jdoGetVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public String toString() {
		return super.toString() + " oid=" + Util.oidToString(jdoZooOid) + " state=" + status; 
	}
} // end class definition

