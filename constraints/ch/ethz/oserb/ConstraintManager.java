package ch.ethz.oserb;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
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
import javax.jdo.listener.ClearLifecycleListener;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.DeleteLifecycleListener;
import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;
import javax.jdo.listener.InstanceLifecycleListener;
import javax.jdo.listener.LoadLifecycleListener;
import javax.jdo.listener.StoreLifecycleListener;

import net.sf.oval.ConstraintViolation;
import net.sf.oval.Validator;
import net.sf.oval.configuration.annotation.AnnotationsConfigurer;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.xml.XMLConfigurer;
import net.sf.oval.internal.Log;

import org.zoodb.api.impl.ZooPCImpl;

import tudresden.ocl20.pivot.interpreter.IInterpretationResult;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.model.ModelAccessException;
import tudresden.ocl20.pivot.modelinstance.IModelInstance;
import tudresden.ocl20.pivot.modelinstancetype.exception.TypeNotFoundInModelException;
import tudresden.ocl20.pivot.modelinstancetype.java.internal.modelinstance.JavaModelInstance;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.pivotmodel.Constraint;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.standardlibrary.java.internal.library.JavaOclBoolean;
import tudresden.ocl20.pivot.tools.template.exception.TemplateException;
import ch.ethz.oserb.configurer.OCLConfigurer;
import ch.ethz.oserb.expression.ExpressionLanguageOclImpl;

public class ConstraintManager implements PersistenceManager {

	private static Validator validator;
	private static PersistenceManager pm;
	private static final Log LOG = Log.getLog(ConstraintManager.class);
	
	// resources
	private static List<Constraint> oclConstraints;
	
	private static IModel model;
	private static IModelInstance modelInstance;
	
	/**
	 * Constraint Manager Constructor:
	 * Registers instance lifecycle listeners at persistence manager.
	 * Initializes validator for annotations and POJOs.
	 * 
	 * @param PersistenceManager
	 */
	public ConstraintManager(PersistenceManager pm) {
		
		// register persistence manager and listeners
		setPersistenceManager(pm);
		addInstanceLifecycleListener(pm);
		
		// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer());	
	}
	
	/**
	 * Constraint Manager Constructor:
	 * Registers instance lifecycle listeners at persistence manager.
	 * Initializes validator for annotations, POJOs and XML config file.
	 * 
	 * @param PersistenceManager
	 * @param File xmlConfig
	 * @throws IOException 
	 */
	public ConstraintManager(PersistenceManager pm, File xmlConfig) throws IOException {
		
		// register persistence manager and listeners
		setPersistenceManager(pm);
		addInstanceLifecycleListener(pm);
		
		// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer(), new XMLConfigurer(xmlConfig));
	}	
	
	/**
	 * Constraint Manager Constructor:
	 * Registers instance lifecycle listeners at persistence manager.
	 * Initializes validator for annotations, POJOs and OCL config file.
	 * Registers OCl as supported annotation Language, requires modelProviderClass.
	 * See Dresden OCL documentation for proper usage.
	 * 
	 * @param PersistenceManager
	 * @param File oclConfig
	 * @param File modelProviderClass
	 * @throws IOException 
	 * @throws TemplateException 
	 * @throws ModelAccessException 
	 * @throws ParseException 
	 */
	public ConstraintManager(PersistenceManager pm, File oclConfig, File modelProviderClass) throws IOException, TemplateException, ModelAccessException, ParseException {

		// register persistence manager and listeners
		setPersistenceManager(pm);
		addInstanceLifecycleListener(pm);
		
		// initialize ocl parser
		StandaloneFacade.INSTANCE.initialize(new URL("file:"+ new File("log4j.properties").getAbsolutePath()));
		model = StandaloneFacade.INSTANCE.loadJavaModel(modelProviderClass);
		oclConstraints = StandaloneFacade.INSTANCE.parseOclConstraints(model, oclConfig);
		
		// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer());
		validator.getExpressionLanguageRegistry().registerExpressionLanguage("ocl", new ExpressionLanguageOclImpl(model));
	}	
	
	/*
	 *	 validation wrapper
	 */
	public static boolean validate(Object obj){
		boolean valid = true;
		// Oval validator
		List<ConstraintViolation> violations = validator.validate(obj);
		if (violations.size() > 0) {
			valid = false;
			for(ConstraintViolation violation:violations){
				LOG.warn(violation.getMessage());
			}
		}
		
		// Dresden OCL validator
		if(model != null){
			try {
				// create model instance
				modelInstance = new JavaModelInstance(model);	
				modelInstance.addModelInstanceElement(obj);
				// interpret OCL constraints
				for (IInterpretationResult result : StandaloneFacade.INSTANCE.interpretEverything(modelInstance, oclConstraints)) {
					if(((JavaOclBoolean)result.getResult()).isTrue()){
						Constraint constraint = result.getConstraint();
						LOG.warn(obj.getClass()+" does not satisfy condition:\ncontext "+obj.getClass().getSimpleName()+ " "+result.getConstraint().getSpecification().getBody());
					}
					valid &= ((JavaOclBoolean)result.getResult()).isTrue();
				}
			} catch (TypeNotFoundInModelException e) {
				LOG.error("Object type not part of model!");
			}
		}
		return valid;
	}

	public void setPersistenceManager(PersistenceManager pm){
		this.pm = pm;
	}
	
	public PersistenceManager getPersistenceManager(){
		return pm;
	}
	
	private void addInstanceLifecycleListener(PersistenceManager pm){
		pm.addInstanceLifecycleListener(new ListenerClear(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerCreate(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerDelete(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerDirty(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerLoad(), (Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerStore(),(Class<?>) null);
	}
	
	private static class ListenerLoad implements LoadLifecycleListener {
		@Override
		public void postLoad(InstanceLifecycleEvent arg0) {
			//System.out.println("postLoad");
		}
	}

	private static class ListenerStore implements StoreLifecycleListener {
		@Override
		public void postStore(InstanceLifecycleEvent arg0) {
			//System.out.println("postStore");
		}

		@Override
		public void preStore(InstanceLifecycleEvent event) {
			// defered evaluation
			Object obj = event.getSource();
			validate(obj);
			//System.out.println("preStore");
		}
	}

	private static class ListenerClear implements ClearLifecycleListener {
		@Override
		public void postClear(InstanceLifecycleEvent arg0) {
			//System.out.println("postClear");
		}

		@Override
		public void preClear(InstanceLifecycleEvent arg0) {
			//System.out.println("preClear");
		}
	}

	private static class ListenerDelete implements DeleteLifecycleListener {
		@Override
		public void postDelete(InstanceLifecycleEvent arg0) {
			//System.out.println("postDelete");
		}

		@Override
		public void preDelete(InstanceLifecycleEvent arg0) {
			//System.out.println("preDelete");
		}
	}

	private static class ListenerCreate implements CreateLifecycleListener {
		@Override
		public void postCreate(InstanceLifecycleEvent event) {
			//System.out.println("postCreate");
		}
	}

	private static class ListenerDirty implements DirtyLifecycleListener {
		@Override
		public void postDirty(InstanceLifecycleEvent arg0) {
			//System.out.println("postDirty");
		}

		@Override
		public void preDirty(InstanceLifecycleEvent arg0) {
			//System.out.println("preDirty");
		}
	}

	@Override
	public void addInstanceLifecycleListener(InstanceLifecycleListener arg0,
			Class... arg1) {
		pm.addInstanceLifecycleListener(arg0, arg1);
		
	}

	@Override
	public void checkConsistency() {
		pm.checkConsistency();
		
	}

	@Override
	public void close() {
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
/*
 * // managed objects: filter out class defs Set mobs = pm.getManagedObjects();
 * int i= 0; Iterator itr = mobs.iterator(); while(itr.hasNext()){ Object obj =
 * itr.next(); if(obj.getClass().equals(ZooClassDef.class))continue;
 * if(((ZooPCImpl)obj).jdoZooIsDirty()) i++; } System.out.println("dirty: "+i);
 */

/*
 * // managed objects: filter class Set mobs = pm.getManagedObjects(); int i= 0;
 * System.out.println(mobs.size()); Iterator itr = mobs.iterator();
 * while(itr.hasNext()){ Object elem = itr.next();
 * if(elem.getClass().equals(extent)){ ZooPCImpl p = extent.cast(elem);
 * if(p.jdoZooIsDirty())i++; } } System.out.println("size: "+i);
 */