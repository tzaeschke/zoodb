package ch.ethz.oserb;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.listener.ClearLifecycleListener;
import javax.jdo.listener.CreateLifecycleListener;
import javax.jdo.listener.DeleteLifecycleListener;
import javax.jdo.listener.DirtyLifecycleListener;
import javax.jdo.listener.InstanceLifecycleEvent;
import javax.jdo.listener.LoadLifecycleListener;
import javax.jdo.listener.StoreLifecycleListener;

import net.sf.oval.Check;
import net.sf.oval.ConstraintViolation;
import net.sf.oval.Validator;
import net.sf.oval.configuration.Configurer;
import net.sf.oval.configuration.annotation.AnnotationsConfigurer;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.xml.XMLConfigurer;
import net.sf.oval.constraint.MinCheck;
import net.sf.oval.guard.Guard;

import org.zoodb.api.impl.ZooPCImpl;

import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.tools.template.exception.TemplateException;
import ch.ethz.oserb.example.ExamplePerson;
import ch.ethz.oserb.expression.ExpressionLanguageOclImpl;

public class ConstraintManager {

	private static Validator validator;
	private static XMLConfigurer xmlConfigurer;
	private static PersistenceManager pm;

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
	/* initializes the xml configuration and validator */
	public void initialize(File file) throws MalformedURLException, TemplateException{
		// config
		try {
			xmlConfigurer = new XMLConfigurer(file);		
			validator = new Validator(new AnnotationsConfigurer(), xmlConfigurer);
		} catch (IOException e) {
			System.out.println("Could not open configuration file!");
			validator = new Validator(new AnnotationsConfigurer());
		}
		// interpreter 	
    	StandaloneFacade.INSTANCE.initialize(new URL("file:"+ new File("log4j.properties").getAbsolutePath()));
		
		/*Playground*/
		
		// addCheck at runtime (POJO)
		/*MinCheck min = new MinCheck();
		min.setTarget("age");
		min.setMin(2.0);
		validator.addChecks(ExamplePerson.class, min);*/
				
		// register OCL Expression Language Implementation
		validator.getExpressionLanguageRegistry().registerExpressionLanguage("ocl", new ExpressionLanguageOclImpl());
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
			ZooPCImpl obj = (ZooPCImpl) event.getSource();
			List<ConstraintViolation> violations = validator.validate(obj);

			if (violations.size() > 0) {
				for(ConstraintViolation violation:violations){
					System.out.println("violation: "+violation.getMessage());
				}
			}
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