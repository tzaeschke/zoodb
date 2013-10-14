package ch.ethz.oserb;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.listener.ClearLifecycleListener;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.DeleteLifecycleListener;
import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;
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

public class ConstraintManager {

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
		this.pm = pm;

		// register listeners
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
		this.pm = pm;

		// register listeners
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
		this.pm = pm;

		// register listeners
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