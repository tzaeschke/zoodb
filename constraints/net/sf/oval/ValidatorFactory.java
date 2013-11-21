package net.sf.oval;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.ResourceBundle;

import net.sf.oval.configuration.annotation.AnnotationsConfigurer;
import net.sf.oval.configuration.ocl.OCLConfig;
import net.sf.oval.configuration.ocl.OCLConfigurer;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.xml.XMLConfigurer;
import net.sf.oval.exception.ExpressionLanguageNotAvailableException;
import net.sf.oval.expression.ExpressionLanguageOclImpl;
import net.sf.oval.internal.ClassChecks;
import net.sf.oval.localization.message.ResourceBundleMessageResolver;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.model.ModelAccessException;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.tools.template.exception.TemplateException;

public class ValidatorFactory {
	
	private Validator validator;
	
	public Validator getValidator(){
		Map<Class<?>,ClassChecks> map = validator.getChecksByClass();
		//map.get(Student.class).checksForObject.
		Validator val = new Validator(map,validator.getConfigurers());
		try{
			val.getExpressionLanguageRegistry().registerExpressionLanguage("ocl",validator.getExpressionLanguageRegistry().getExpressionLanguage("ocl"));
		}catch (ExpressionLanguageNotAvailableException e){
			// don't worry, validator didn't support ocl
		}
		return val;
	}
	
    /**
     * initializes factory for ocl configuration.
     * 
     * @param pm
     * @param modelProviderClass
     * @param oclConfigs
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws TemplateException
     * @throws ModelAccessException
     * @throws ParseException
     */
    public ValidatorFactory(File modelProviderClass, ArrayList<OCLConfig> oclConfigs) throws ClassNotFoundException, IOException, TemplateException, ModelAccessException, ParseException{
    	
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
     * @throws IOException
     */
    public ValidatorFactory(File xmlConfig) throws IOException{    	
    	// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer(), new XMLConfigurer(xmlConfig));
    }
    
    /**
     * initializes factory for standard usage.
     * only built-in expression languages, no config file.
     * 
     * @param p_m
     */
    public ValidatorFactory(){
    	// initialize validator
		validator = new Validator(new AnnotationsConfigurer(), new POJOConfigurer());	
    }
    
	/**
     * clears the checks and constraint sets
     * =>a reconfiguration using the currently registered configurers will automatically happen
     *
     */
    public void reconfigureChecks(){
    	validator.reconfigureChecks();
    }
    
    // object level checks
    
    /**
     * Gets the object-level constraint checks for the given class.
     * @param clazz
     */
    public Check[] getChecks(Class<?> clazz){
    	return validator.getChecks(clazz);
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
    public Check[] getChecks(Field field){
    	return validator.getChecks(field);
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
    public Check[] getChecks(Method method){
    	return validator.getChecks(method);
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
    public ConstraintSet getConstraintSet(String constraintSetId){
    	return validator.getConstraintSet(constraintSetId);
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
    
}
