/**
 * 
 */
package ch.ethz.oserb;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ResourceBundle;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import net.sf.oval.Check;
import net.sf.oval.ConstraintSet;
import net.sf.oval.Validator;
import net.sf.oval.configuration.annotation.AnnotationsConfigurer;
import net.sf.oval.configuration.ocl.OCLConfig;
import net.sf.oval.configuration.ocl.OCLConfigurer;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.xml.XMLConfigurer;
import net.sf.oval.exception.ExpressionLanguageNotAvailableException;
import net.sf.oval.expression.ExpressionLanguage;
import net.sf.oval.expression.ExpressionLanguageOclImpl;
import net.sf.oval.expression.ExpressionLanguageRegistry;
import net.sf.oval.localization.message.ResourceBundleMessageResolver;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.model.ModelAccessException;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.tools.template.exception.TemplateException;
import ch.ethz.oserb.ConstraintManager.CouplingMode;

/**
 * @author Benji
 *
 */
public class ConstraintManagerFactory {
	private static PersistenceManagerFactory persistenceManagerFactory;
	private static Validator validator;
	private static CouplingMode cmode;
	private static LinkedList<PersistenceManager> pms = new LinkedList<PersistenceManager>();
	/**
	 * constraint manager factory.
	 * "...each PersistenceManager has its own transaction..."
	 * so each ConstraintManager has its own PersistenceManager.
	 * 
	 * @return ConstraintManager
	 * 
	 */
    public static ConstraintManager getConstraintManager() throws Exception {
    	// "each PersistenceManager has its own transaction"
    	Validator val = new Validator(validator.getConfigurers());
    	try{
    		// try to bind ocl (if available)
    		val.getExpressionLanguageRegistry().registerExpressionLanguage("ocl", validator.getExpressionLanguageRegistry().getExpressionLanguage("ocl"));
    	}catch (ExpressionLanguageNotAvailableException e){
    		// no worries, ocl was not configured
    	}
    	
    	// keep track of created persistence managers 
    	PersistenceManager pm = persistenceManagerFactory.getPersistenceManager();
    	pms.add(pm);
    	
    	return new ConstraintManager(pm,val, cmode);
    }
    
    /**
     * initializes factory for ocl configuration.
     * 
     * @param pm
     * @param modelProviderClass
     * @param oclConfigs
     * @param couplingMode
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws TemplateException
     * @throws ModelAccessException
     * @throws ParseException
     */
    public static void initialize(PersistenceManagerFactory pmf, File modelProviderClass, ArrayList<OCLConfig> oclConfigs, CouplingMode couplingMode) throws ClassNotFoundException, IOException, TemplateException, ModelAccessException, ParseException{
		//
    	persistenceManagerFactory = pmf;
    	cmode = couplingMode;
    	
    	// prepare resource bundle
		ResourceBundleMessageResolver resolver = (ResourceBundleMessageResolver) Validator.getMessageResolver();
		resolver.addMessageBundle(ResourceBundle.getBundle("net.sf.oval.OclMessages"));
    	
    	// initialize ocl parser
		StandaloneFacade.INSTANCE.initialize(new URL("file:"+ new File("log4j.properties").getAbsolutePath()));
		IModel model = StandaloneFacade.INSTANCE.loadJavaModel(modelProviderClass);
		
		// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer(), new OCLConfigurer(oclConfigs));
		validator.getExpressionLanguageRegistry().registerExpressionLanguage("ocl", new ExpressionLanguageOclImpl(model));
    }
    
    /**
     * initializes factory for xml configuration.
     *  
     * @param pm
     * @param xmlConfig
     * @param couplingMode
     * @throws IOException
     */
    public static void initialize(PersistenceManagerFactory pmf, File xmlConfig, CouplingMode couplingMode) throws IOException{
		// 
    	persistenceManagerFactory = pmf;
    	cmode = couplingMode;
    	
    	// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer(), new XMLConfigurer(xmlConfig));
    }
    
    /**
     * initializes factory for standard usage.
     * only built-in expression languages, no config file.
     * 
     * @param p_m
     * @param couplingMode
     */
    public static void initialize(PersistenceManagerFactory pmf, CouplingMode couplingMode){
		// 
    	persistenceManagerFactory = pmf;
    	cmode = couplingMode;
    	
    	// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer());	
    }
	/**
     * clears the checks and constraint sets
     * =>a reconfiguration using the currently registered configurers will automatically happen
     *
     */
    public void reconfigure(){
    	validator.reconfigureChecks();
    }
    
    // object level checks
    
    /**
     * Gets the object-level constraint checks for the given class.
     * @param clazz
     */
    public void getChecks(Class<?> clazz){
    	validator.getChecks(clazz);
    }
    
    /**
     * Gets the object-level constraint checks for the given class
     * @param clazz
     * @param checks
     */
    public void addChecks(Class<?> clazz, Check checks){
    	validator.addChecks(clazz, checks);
    }
   
    /**
     * Removes object-level constraint checks.
     * @param clazz
     * @param checks
     */
    public void removeChecks(Class<?> clazz, Check checks){
    	validator.removeChecks(clazz, checks);
    }
    
    // field level checks
    /**
     * Gets the constraint checks for the given field
     * @param field
     */
    public void getChecks(Field field){
    	validator.getChecks(field);
    }
    
    /**
     * Registers constraint checks for the given field.
     * @param field
     * @param checks
     */
    public void addChecks(Field field, Check checks){
    	validator.addChecks(field, checks);
    }
    
    /**
     * Removes constraint checks for the given field.
     * @param field
     * @param checks
     */
    public void removeChecks(Field field, Check checks){
    	validator.removeChecks(field, checks);
    }
    
    // method level checks
    /**
     * Gets the constraint checks for the given method's return value.
     * @param method
     */
    public void getChecks(Method method){
    	validator.getChecks(method);
    }
    
    /**
     * Registers constraint checks for the given getter's return value
     * @param invariantMethod
     * @param checks
     */
    public void addChecks(Method invariantMethod, Check checks){
    	validator.addChecks(invariantMethod, checks);
    }
    
    /**
     * Removes constraint checks for the given getter's return value.
     * @param getter
     * @param checks
     */
    public void removeChecks(Method getter, Check checks){
    	validator.removeChecks(getter, checks);
    }
    
    // constraint set checks
    /**
     * Returns the given constraint set.
     * @param constraintSetId
     */
    public void getConstraintSet(String constraintSetId){
    	validator.getConstraintSet(constraintSetId);
    }
    
    /**
     * Returns the given constraint set.
     * @param constraintset
     * @param overwrite
     */
    public void addConstraintSet(ConstraintSet constraintset, Boolean overwrite){
    	validator.addConstraintSet(constraintset, overwrite);
    }
    
    /**
     * Removes the constraint set with the given id
     * @param constraintSetId
     */
    public void removeConstraintSet(String constraintSetId){
    	validator.removeConstraintSet(constraintSetId);
    }
    
    /**
     * removes the specified persistence manager from the tracking list.
     */
    public static boolean removePersistenceManager(PersistenceManager pm){
    	return pms.remove(pm);
    
    }
    
    /**
     * get the persistence manager tracking list.
     * @return LinkedList<PersistenceManager>
     */
    public static LinkedList<PersistenceManager> getPersistenceManagerList(){
    	return pms;
    }
}
