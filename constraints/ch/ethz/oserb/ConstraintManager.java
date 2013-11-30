package ch.ethz.oserb;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.jdo.Extent;
import javax.jdo.FetchGroup;
import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.ObjectState;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;
import javax.jdo.listener.InstanceLifecycleListener;

import net.sf.oval.ConstraintViolation;
import net.sf.oval.Validator;
import net.sf.oval.exception.ConstraintsViolatedException;
import net.sf.oval.internal.Log;

public class ConstraintManager implements PersistenceManager,Serializable {

	private static final long serialVersionUID = -300390607650339029L;

	public enum CouplingMode {IMMEDIATE, DEFERRED};
	private static CouplingMode couplingMode;

	private static PersistenceManager pm;
	private static Validator validator;
	private static final Log LOG = Log.getLog(ConstraintManager.class);	
    
    public ConstraintManager(PersistenceManager pm,Validator validator, CouplingMode couplingMode) {
		setPersistenceManager(pm);
		addInstanceLifecycleListener(pm);
		setCouplingMode(couplingMode);
		setValidator(pm,validator);
	}
    
    /**
     * commit the current transaction.
     * for deferred coupling mode, evaluation will be invoked first,
     * i.e. all new or dirty objects in the managed objects set get validated,
     * and in case of violations an exception will be thrown.
     * hence not guaranteed to commit! use forceCommit() instead.
     * 
     * @throws ConstraintsViolatedException
     */
	public void commit() throws ConstraintsViolatedException{
		// deferred constraint evaluation
		if(couplingMode==CouplingMode.DEFERRED){
			List<ConstraintViolation> constraintViolations = new LinkedList<ConstraintViolation>();
			for(Object obj:pm.getManagedObjects(EnumSet.of(ObjectState.PERSISTENT_DIRTY, ObjectState.PERSISTENT_NEW))){
				constraintViolations.addAll((validate(obj)));
			}
			if(constraintViolations.size()>0)	throw new ConstraintsViolatedException(constraintViolations);
		}
		// if no constraint violated -> commit
		pm.currentTransaction().commit();
	}
	
	// shortcuts
	/**
	 * forces commit.
	 * no constraint evaluation will be carried out.
	 */
	public void forceCommit(){
		pm.currentTransaction().commit();
	}
	
	/**
	 * shortcurt for currentTransaction().rollback()
	 */
	
	public void abort(){
		pm.currentTransaction().rollback();
	}
	
	/**
	 *  shortcut for currentTransaction().begin();
	 */
	public void begin(){
		pm.currentTransaction().begin();
	}
	
	/**
	 * set the coupling mode.
	 * @param couplingMode
	 */	
	public void setCouplingMode(CouplingMode mode){
		couplingMode = mode;
	}
	
	/**
	 * get the currently set coupling mode.
	 * @return CouplingMode
	 */
	
	public CouplingMode getCouplingMode(){
		return couplingMode;
	}
	
	
	/**
	 *	 validates the specified object.
	 *
	 *	@param object to validate
	 */
	public static List<ConstraintViolation> validate(Object obj){
		return validator.validate(obj);
	}
	
	/**
	 * simple immediate check.
	 * 
	 * @param obj object to validate
	 * @return boolean valid
	 */
	public boolean isValid(Object obj){
		return validate(obj).size()==0;
	}
	
	/**
	 * immediate check of the specified objects.
	 * 
	 * @param obj object to validate
	 * @throws ConstraintException violations
	 */
	public static void validateImmediate(Object obj) throws ConstraintsViolatedException{
		List<ConstraintViolation> constraintViolations = validate(obj);
		if(constraintViolations.size()>0) throw new ConstraintsViolatedException(constraintViolations);
	}
	
	/**
	 * immediate check of all managed objects.
	 * 
	 * @throws ConstraintException violations
	 */
	public void validateImmediate() throws ConstraintsViolatedException{
		List<ConstraintViolation> constraintViolations = new LinkedList<ConstraintViolation>();		
		for(Object obj:pm.getManagedObjects(EnumSet.of(ObjectState.PERSISTENT_DIRTY, ObjectState.PERSISTENT_NEW))){
			constraintViolations.addAll((validate(obj)));
		}
		if(constraintViolations.size()>0)	throw new ConstraintsViolatedException(constraintViolations);
	}

	/**
	 * disables the specified profile
	 * @param profile
	 */
	public void disableProfile(String profile){
		validator.disableProfile(profile);
	}
	
	/**
	 * enables the specified profile
	 * @param profile
	 */
	public void enableProfile(String profile){
		validator.enableProfile(profile);
	}

	/**
	 * disables all profiles
	 */
	public void disableAllProfiles(){
		validator.disableAllProfiles();
	}
	
	/**
	 * enables all profiles.
	 */
	public void enableAllProfiles(){
		validator.enableAllProfiles();
	}
	
	/**
	 * set the persistence manager
	 * @param pm
	 */
	private void setPersistenceManager(PersistenceManager pm){
		this.pm = pm;
	}
	
	/**
	 * get the persistence manager.
	 * @return
	 */
	public PersistenceManager getPersistenceManager(){
		return pm;
	}
	
	public void setValidator(PersistenceManager pm, Validator validator){
		validator.setPersistenceManager(pm);
		this.validator = validator;
	}
	
	/**
	 * add the lifecycle listeners necessary for immediate evaluation
	 * @param pm
	 */
	private void addInstanceLifecycleListener(PersistenceManager pm){
		// immediate constraint evaluation
		pm.addInstanceLifecycleListener(new ListenerCreate(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerDirty(),(Class<?>) null);
	}

	/**
	 * listener for newly persisted objects.
	 * only used for immediate coupling mode.
	 * @author oserb
	 *
	 */
	private static class ListenerCreate implements CreateLifecycleListener {
		@Override
		public void postCreate(InstanceLifecycleEvent event) {
			// immediate evaluation
			if(couplingMode==CouplingMode.IMMEDIATE)validateImmediate(event.getSource());
		}
	}
	/**
	 * listener for dirty persisted objects.
	 * only used for immediate coupling mode.
	 * @author oserb
	 *
	 */
	private static class ListenerDirty implements DirtyLifecycleListener {
		@Override
		public void postDirty(InstanceLifecycleEvent event) {
			// immediate evaluation
			if(couplingMode==CouplingMode.IMMEDIATE)validateImmediate(event.getSource());
		}

		@Override
		public void preDirty(InstanceLifecycleEvent event) {
			// nothing changed yet
		}
	}
	
	/*
	 * Delegation of persistence manager functionality
	 */
	
	@Override
	public void addInstanceLifecycleListener(InstanceLifecycleListener arg0, Class... arg1) {
		pm.addInstanceLifecycleListener(arg0, arg1);
		
	}

	@Override
	public void checkConsistency() {
		pm.checkConsistency();
		
	}

	@Override
	public void close() {
		// remove this persistence manager from tracking list
		ConstraintManagerFactory.removeConstraintManager(this);
		pm.close();
		
	}

	@Override
	public Transaction currentTransaction() {
		return pm.currentTransaction();
	}

	@Override
	public void deletePersistent(Object arg0) {
		pm.deletePersistent(arg0);
		
	}

	@Override
	public void deletePersistentAll(Object... arg0) {
		pm.deletePersistentAll(arg0);
		
	}

	@Override
	public void deletePersistentAll(Collection arg0) {
		pm.deletePersistentAll(arg0);
		
	}

	@Override
	public <T> T detachCopy(T arg0) {
		return pm.detachCopy(arg0);
	}

	@Override
	public <T> Collection<T> detachCopyAll(Collection<T> arg0) {
		return pm.detachCopyAll(arg0);
	}

	@Override
	public <T> T[] detachCopyAll(T... arg0) {
		return pm.detachCopyAll(arg0);
	}

	@Override
	public void evict(Object arg0) {
		pm.evict(arg0);
		
	}

	@Override
	public void evictAll() {
		pm.evictAll();
		
	}

	@Override
	public void evictAll(Object... arg0) {
		pm.evictAll(arg0);
		
	}

	@Override
	public void evictAll(Collection arg0) {
		pm.evictAll(arg0);
		
	}

	@Override
	public void evictAll(boolean arg0, Class arg1) {
		pm.evictAll(arg0, arg1);
		
	}

	@Override
	public void flush() {
		pm.flush();
		
	}

	@Override
	public boolean getCopyOnAttach() {
		return pm.getCopyOnAttach();
	}

	@Override
	public JDOConnection getDataStoreConnection() {
		return pm.getDataStoreConnection();
	}

	@Override
	public Integer getDatastoreReadTimeoutMillis() {
		return pm.getDatastoreReadTimeoutMillis();
	}

	@Override
	public Integer getDatastoreWriteTimeoutMillis() {
		return pm.getDatastoreWriteTimeoutMillis();
	}

	@Override
	public boolean getDetachAllOnCommit() {
		return pm.getDetachAllOnCommit();
	}

	@Override
	public <T> Extent<T> getExtent(Class<T> arg0) {
		return pm.getExtent(arg0);
	}

	@Override
	public <T> Extent<T> getExtent(Class<T> arg0, boolean arg1) {
		return pm.getExtent(arg0, arg1);
	}

	@Override
	public FetchGroup getFetchGroup(Class arg0, String arg1) {
		return pm.getFetchGroup(arg0, arg1);
	}

	@Override
	public FetchPlan getFetchPlan() {
		return pm.getFetchPlan();
	}

	@Override
	public boolean getIgnoreCache() {
		return pm.getIgnoreCache();
	}

	@Override
	public Set getManagedObjects() {
		return pm.getManagedObjects();
	}

	@Override
	public Set getManagedObjects(EnumSet<ObjectState> arg0) {
		return pm.getManagedObjects(arg0);
	}

	@Override
	public Set getManagedObjects(Class... arg0) {
		return pm.getManagedObjects(arg0);
	}

	@Override
	public Set getManagedObjects(EnumSet<ObjectState> arg0, Class... arg1) {
		return pm.getManagedObjects(arg0, arg1);
	}

	@Override
	public boolean getMultithreaded() {
		return pm.getMultithreaded();
	}

	@Override
	public Object getObjectById(Object arg0) {
		return pm.getObjectById(arg0);
	}

	@Override
	public Object getObjectById(Object arg0, boolean arg1) {
		return getObjectById(arg0, arg1);
	}

	@Override
	public <T> T getObjectById(Class<T> arg0, Object arg1) {
		return pm.getObjectById(arg0, arg1);
	}

	@Override
	public Object getObjectId(Object arg0) {
		return pm.getObjectId(arg0);
	}

	@Override
	public Class getObjectIdClass(Class arg0) {
		return pm.getObjectIdClass(arg0);
	}

	@Override
	public Collection getObjectsById(Collection arg0) {
		return pm.getObjectsById(arg0);
	}

	@Override
	public Object[] getObjectsById(Object... arg0) {
		return pm.getObjectsById(arg0);
	}

	@Override
	public Collection getObjectsById(Collection arg0, boolean arg1) {
		return pm.getObjectsById(arg0, arg1);
	}

	@SuppressWarnings("deprecation")
	@Override
	public Object[] getObjectsById(Object[] arg0, boolean arg1) {
		return pm.getObjectsById(arg0, arg1);
	}

	@Override
	public Object[] getObjectsById(boolean arg0, Object... arg1) {
		return pm.getObjectsById(arg0, arg1);
	}

	@Override
	public PersistenceManagerFactory getPersistenceManagerFactory() {
		return pm.getPersistenceManagerFactory();
	}

	@Override
	public Sequence getSequence(String arg0) {
		return pm.getSequence(arg0);
	}

	@Override
	public Date getServerDate() {
		return pm.getServerDate();
	}

	@Override
	public Object getTransactionalObjectId(Object arg0) {
		return pm.getTransactionalObjectId(arg0);
	}

	@Override
	public Object getUserObject() {
		return pm.getUserObject();
	}

	@Override
	public Object getUserObject(Object arg0) {
		return pm.getUserObject(arg0);
	}

	@Override
	public boolean isClosed() {
		return pm.isClosed();
	}

	@Override
	public void makeNontransactional(Object arg0) {
		pm.makeNontransactional(arg0);
		
	}

	@Override
	public void makeNontransactionalAll(Object... arg0) {
		pm.makeNontransactionalAll(arg0);
		
	}

	@Override
	public void makeNontransactionalAll(Collection arg0) {
		pm.makeNontransactionalAll(arg0);
		
	}

	@Override
	public <T> T makePersistent(T arg0) {
		return pm.makePersistent(arg0);
	}

	@Override
	public <T> T[] makePersistentAll(T... arg0) {
		return pm.makePersistentAll(arg0);
	}

	@Override
	public <T> Collection<T> makePersistentAll(Collection<T> arg0) {
		return pm.makePersistentAll(arg0);
	}

	@Override
	public void makeTransactional(Object arg0) {
		pm.makeTransactional(arg0);
		
	}

	@Override
	public void makeTransactionalAll(Object... arg0) {
		pm.makeTransactional(arg0);
		
	}

	@Override
	public void makeTransactionalAll(Collection arg0) {
		pm.makeTransactionalAll(arg0);
		
	}

	@Override
	public void makeTransient(Object arg0) {
		pm.makeTransient(arg0);
		
	}

	@Override
	public void makeTransient(Object arg0, boolean arg1) {
		pm.makeTransient(arg0, arg1);
		
	}

	@Override
	public void makeTransientAll(Object... arg0) {
		pm.makeTransientAll(arg0);
		
	}

	@Override
	public void makeTransientAll(Collection arg0) {
		pm.makeTransientAll(arg0);
		
	}

	@SuppressWarnings("deprecation")
	@Override
	public void makeTransientAll(Object[] arg0, boolean arg1) {
		pm.makeTransientAll(arg0, arg1);
		
	}

	@Override
	public void makeTransientAll(boolean arg0, Object... arg1) {
		pm.makeTransientAll(arg0, arg1);
		
	}

	@Override
	public void makeTransientAll(Collection arg0, boolean arg1) {
		pm.makeTransientAll(arg0, arg1);
		
	}

	@Override
	public <T> T newInstance(Class<T> arg0) {
		return pm.newInstance(arg0);
	}

	@Override
	public Query newNamedQuery(Class arg0, String arg1) {
		return pm.newNamedQuery(arg0, arg1);
	}

	@Override
	public Object newObjectIdInstance(Class arg0, Object arg1) {
		return pm.newObjectIdInstance(arg0, arg1);
	}

	@Override
	public Query newQuery() {
		return pm.newQuery();
	}

	@Override
	public Query newQuery(Object arg0) {
		return pm.newQuery(arg0);
	}

	@Override
	public Query newQuery(String arg0) {
		return pm.newQuery(arg0);
	}

	@Override
	public Query newQuery(Class arg0) {
		return pm.newQuery(arg0);
	}

	@Override
	public Query newQuery(Extent arg0) {
		return pm.newQuery(arg0);
	}

	@Override
	public Query newQuery(String arg0, Object arg1) {
		return pm.newQuery(arg0, arg1);
	}

	@Override
	public Query newQuery(Class arg0, Collection arg1) {
		return pm.newQuery(arg0, arg1);
	}

	@Override
	public Query newQuery(Class arg0, String arg1) {
		return pm.newQuery(arg0, arg1);
	}

	@Override
	public Query newQuery(Extent arg0, String arg1) {
		return pm.newQuery(arg0, arg1);
	}

	@Override
	public Query newQuery(Class arg0, Collection arg1, String arg2) {
		return pm.newQuery(arg0, arg1, arg2);
	}

	@Override
	public Object putUserObject(Object arg0, Object arg1) {
		return pm.putUserObject(arg0, arg1);
	}

	@Override
	public void refresh(Object arg0) {
		pm.refresh(arg0);
		
	}

	@Override
	public void refreshAll() {
		pm.refreshAll();
		
	}

	@Override
	public void refreshAll(Object... arg0) {
		pm.refreshAll(arg0);
		
	}

	@Override
	public void refreshAll(Collection arg0) {
		pm.refreshAll(arg0);
		
	}

	@Override
	public void refreshAll(JDOException arg0) {
		pm.refreshAll(arg0);
		
	}

	@Override
	public void removeInstanceLifecycleListener(InstanceLifecycleListener arg0) {
		pm.removeInstanceLifecycleListener(arg0);
		
	}

	@Override
	public Object removeUserObject(Object arg0) {
		return pm.removeUserObject(arg0);
	}

	@Override
	public void retrieve(Object arg0) {
		pm.retrieve(arg0);
		
	}

	@Override
	public void retrieve(Object arg0, boolean arg1) {
		pm.retrieve(arg0, arg1);
		
	}

	@Override
	public void retrieveAll(Collection arg0) {
		pm.retrieveAll(arg0);
		
	}

	@Override
	public void retrieveAll(Object... arg0) {
		pm.retrieveAll(arg0);
		
	}

	@Override
	public void retrieveAll(Collection arg0, boolean arg1) {
		pm.retrieveAll(arg0, arg1);
		
	}

	@SuppressWarnings("deprecation")
	@Override
	public void retrieveAll(Object[] arg0, boolean arg1) {
		pm.retrieveAll(arg0, arg1);
		
	}

	@Override
	public void retrieveAll(boolean arg0, Object... arg1) {
		pm.retrieveAll(arg0, arg1);
		
	}

	@Override
	public void setCopyOnAttach(boolean arg0) {
		pm.setCopyOnAttach(arg0);
		
	}

	@Override
	public void setDatastoreReadTimeoutMillis(Integer arg0) {
		pm.setDatastoreReadTimeoutMillis(arg0);
		
	}

	@Override
	public void setDatastoreWriteTimeoutMillis(Integer arg0) {
		pm.setDatastoreWriteTimeoutMillis(arg0);
		
	}

	@Override
	public void setDetachAllOnCommit(boolean arg0) {
		pm.setDetachAllOnCommit(arg0);
		
	}

	@Override
	public void setIgnoreCache(boolean arg0) {
		pm.setIgnoreCache(arg0);
		
	}

	@Override
	public void setMultithreaded(boolean arg0) {
		pm.setMultithreaded(arg0);
		
	}

	@Override
	public void setUserObject(Object arg0) {
		pm.setUserObject(arg0);
		
	}
}