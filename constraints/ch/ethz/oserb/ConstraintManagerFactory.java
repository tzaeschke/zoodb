/**
 * 
 */
package ch.ethz.oserb;


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedList;

import javax.jdo.PersistenceManagerFactory;

import net.sf.oval.Check;
import net.sf.oval.ConstraintSet;
import net.sf.oval.ValidatorFactory;
import ch.ethz.oserb.ConstraintManager.CouplingMode;

/**
 * @author oserb
 *
 */
public class ConstraintManagerFactory {
	private static PersistenceManagerFactory pmf;
	private static ValidatorFactory vf;
	private static LinkedList<ConstraintManager> cms = new LinkedList<ConstraintManager>();
	
	public static void initialize(PersistenceManagerFactory persistenceManagerFactory, ValidatorFactory validatorFactory){
		pmf = persistenceManagerFactory;
		vf = validatorFactory;
	}
	
	/**
	 * constraint manager factory.
	 * "...each PersistenceManager has its own transaction..."
	 * so each ConstraintManager has its own PersistenceManager.
	 * 
	 * @return ConstraintManager
	 * 
	 */
    public static ConstraintManager getConstraintManager(CouplingMode cmode) throws Exception {
    	// "each PersistenceManager has its own transaction"   	
    	// keep track of created constraint managers 
    	ConstraintManager cm = new ConstraintManager(pmf.getPersistenceManager(),vf.getValidator(), cmode);
    	cms.add(cm);
    	return cm;
    }
    
    
    /**
     * removes the specified constraint manager from the tracking list.
     */
    public static boolean removeConstraintManager(ConstraintManager cm){
    	return cms.remove(cm);
    
    }
    
    /**
     * get the constraint manager tracking list.
     * @return LinkedList<ConstraintManager>
     */
    public static LinkedList<ConstraintManager> getConstraintManagerList(){
    	return cms;
    }
    
	/**
     * clears the checks and constraint sets
     * =>a reconfiguration using the currently registered configurers will automatically happen
     *
     */
    public static void reconfigureChecks(){
    	vf.reconfigureChecks();
    }
    
    // object level checks
    
    /**
     * Gets the object-level constraint checks for the given class.
     * @param clazz
     */
    public static Check[] getChecks(Class<?> clazz){
    	return vf.getChecks(clazz);
    }
    
    /**
     * Gets the object-level constraint checks for the given class
     * @param clazz
     * @param checks
     */
    public static void addChecks(Class<?> clazz, Check checks){
    	vf.addChecks(clazz, checks);
    }
   
    /**
     * Removes object-level constraint checks.
     * @param clazz
     * @param checks
     */
    public static void removeChecks(Class<?> clazz, Check checks){
    	vf.removeChecks(clazz, checks);
    }
    
    // field level checks
    /**
     * Gets the constraint checks for the given field
     * @param field
     */
    public static Check[] getChecks(Field field){
    	return vf.getChecks(field);
    }
    
    /**
     * Registers constraint checks for the given field.
     * @param field
     * @param checks
     */
    public static void addChecks(Field field, Check checks){
    	vf.addChecks(field, checks);
    }
    
    /**
     * Removes constraint checks for the given field.
     * @param field
     * @param checks
     */
    public static void removeChecks(Field field, Check checks){
    	vf.removeChecks(field, checks);
    }
    
    // method level checks
    /**
     * Gets the constraint checks for the given method's return value.
     * @param method
     */
    public static Check[] getChecks(Method method){
    	return vf.getChecks(method);
    }
    
    /**
     * Registers constraint checks for the given getter's return value
     * @param invariantMethod
     * @param checks
     */
    public static void addChecks(Method invariantMethod, Check checks){
    	vf.addChecks(invariantMethod, checks);
    }
    
    /**
     * Removes constraint checks for the given getter's return value.
     * @param getter
     * @param checks
     */
    public static void removeChecks(Method getter, Check checks){
    	vf.removeChecks(getter, checks);
    }
    
    // constraint set checks
    /**
     * Returns the given constraint set.
     * @param constraintSetId
     */
    public static ConstraintSet getConstraintSet(String constraintSetId){
    	return vf.getConstraintSet(constraintSetId);
    }
    
    /**
     * Returns the given constraint set.
     * @param constraintset
     * @param overwrite
     */
    public static void addConstraintSet(ConstraintSet constraintset, Boolean overwrite){
    	vf.addConstraintSet(constraintset, overwrite);
    }
    
    /**
     * Removes the constraint set with the given id
     * @param constraintSetId
     */
    public static void removeConstraintSet(String constraintSetId){
    	vf.removeConstraintSet(constraintSetId);
    }
}
