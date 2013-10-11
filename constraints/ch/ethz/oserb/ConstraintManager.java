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
	private static XMLConfigurer xmlConfigurer;
	private static OCLConfigurer oclConfigurer;
	private static PersistenceManager pm;
	private static final Log LOG = Log.getLog(ConstraintManager.class);
	
	// resources
	private static File oclFile;
	private static List<Constraint> oclConstraints;
	
	private static IModel model;
	private static IModelInstance modelInstance;
	
	/**
	 * constructor.
	 * 
	 * @param PersistenceManager
	 */
	public ConstraintManager(PersistenceManager pm) {
		this.pm = pm;

		// register listeners
		pm.addInstanceLifecycleListener(new ListenerClear(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerCreate(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerDelete(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerDirty(),(Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerLoad(), (Class<?>) null);
		pm.addInstanceLifecycleListener(new ListenerStore(),(Class<?>) null);
	}
	/* initializes the configuration and validator */
	public void initialize(File xmlFile, File oclFile, File classFile) throws MalformedURLException, TemplateException{
		this.oclFile = oclFile;
		
		try {
			// initialize OCL interpreter 	
			StandaloneFacade.INSTANCE.initialize(new URL("file:"+ new File("log4j.properties").getAbsolutePath()));
			// load model
			model = StandaloneFacade.INSTANCE.loadJavaModel(classFile);
			// parse OCL constraints from document
			oclConstraints = StandaloneFacade.INSTANCE.parseOclConstraints(model, oclFile);
		}	catch (IOException e) {
			LOG.error("Could not load ocl file!");
		} catch (ParseException e) {
			LOG.error("Parsing ocl document ("+oclFile.getName()+") failed:\n"+e.getMessage());
		} catch (ModelAccessException e) {
			LOG.error("Could not load model!");
		}
    	
    	// register configurers at validator
		try {
			// load xml configurer
			xmlConfigurer = new XMLConfigurer(xmlFile);	
			// load ocl configurer
			//oclConfigurer = new OCLConfigurer(oclFile,model);
			// register configurers
			validator = new Validator(new AnnotationsConfigurer(), xmlConfigurer);
			// register OCL Expression Language Implementation
			validator.getExpressionLanguageRegistry().registerExpressionLanguage("ocl", new ExpressionLanguageOclImpl(model));
		}catch (IOException e) {
			LOG.error("Could not load config file: "+e.getMessage());
		}
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
		try {
			// create model instance
			modelInstance = new JavaModelInstance(model);	
			modelInstance.addModelInstanceElement(obj);
			// interpret OCL constraints
			for (IInterpretationResult result : StandaloneFacade.INSTANCE.interpretEverything(modelInstance, oclConstraints)) {
				if(((JavaOclBoolean)result.getResult()).isTrue()){
					Constraint constraint = result.getConstraint();
					LOG.warn(obj.getClass()+" does not satisfy condition: "+result.getResult());
				}
				valid &= ((JavaOclBoolean)result.getResult()).isTrue();
			}
		} catch (TypeNotFoundInModelException e) {
			LOG.error("Object type not part of model!");
		}
		return valid;
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