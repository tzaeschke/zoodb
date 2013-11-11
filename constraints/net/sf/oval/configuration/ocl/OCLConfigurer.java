/**
 * 
 */
package net.sf.oval.configuration.ocl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.oval.Check;
import net.sf.oval.Validator;
import net.sf.oval.configuration.Configurer;
import net.sf.oval.configuration.ocl.FSM.STATE;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.pojo.elements.ClassConfiguration;
import net.sf.oval.configuration.pojo.elements.ConstraintSetConfiguration;
import net.sf.oval.configuration.pojo.elements.ObjectConfiguration;
import net.sf.oval.constraint.OclConstraintCheck;
import net.sf.oval.constraint.OclConstraintsCheck;
import net.sf.oval.exception.InvalidConfigurationException;
import net.sf.oval.internal.Log;

/**
 * @author oserb
 *
 */
public class OCLConfigurer implements Configurer, Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2691480497984937837L;
	
	private POJOConfigurer pojoConfigurer = new POJOConfigurer();
	private static final Log LOG = Log.getLog(Validator.class);
	
	public OCLConfigurer(ArrayList<OCLConfig> oclConfigs) throws FileNotFoundException, IOException, ClassNotFoundException{
		LOG.info("OCL configurer registered: {1}", this.getClass().getName());
		
		for(OCLConfig oclConfig:oclConfigs){
			streamTokenizer(oclConfig.getOclFile(), oclConfig.getProfiles(), oclConfig.getSeverity());
		}

	}
	/* (non-Javadoc)
	 * @see net.sf.oval.configuration.Configurer#getClassConfiguration(java.lang.Class)
	 */
	@Override
	public ClassConfiguration getClassConfiguration(Class<?> clazz)	throws InvalidConfigurationException {
		
		return pojoConfigurer.getClassConfiguration(clazz);
	}

	/* (non-Javadoc)
	 * @see net.sf.oval.configuration.Configurer#getConstraintSetConfiguration(java.lang.String)
	 */
	@Override
	public ConstraintSetConfiguration getConstraintSetConfiguration(String constraintSetId) throws InvalidConfigurationException {
		
		return pojoConfigurer.getConstraintSetConfiguration(constraintSetId);
	}
	
	private void createAssert(String expr, String context, int severity, String profiles) throws ClassNotFoundException{
		// get reference to class configurations
		Set<ClassConfiguration> classConfigurations = pojoConfigurer.getClassConfigurations();
		if(classConfigurations == null){
			// setup new class configuration
			classConfigurations = new HashSet<ClassConfiguration>();					
			pojoConfigurer.setClassConfigurations(classConfigurations);
		}
		
		// get reference to class configuration
		ClassConfiguration classConfiguration = pojoConfigurer.getClassConfiguration(Class.forName(context));
		if(classConfiguration == null){
			// setup class configuration			
			classConfiguration = new ClassConfiguration();
			classConfigurations.add(classConfiguration);
			classConfiguration.type = Class.forName(context);
		}
		
		// get reference to object configuration
		ObjectConfiguration objectConfiguration = classConfiguration.objectConfiguration;
		if(objectConfiguration == null){
			// setup object configuration
			objectConfiguration = new ObjectConfiguration();
			classConfiguration.objectConfiguration = objectConfiguration;
			objectConfiguration.checks = new ArrayList<Check>();
		}
		
		// get reference to OclConstraintsCheck
		List<Check> checks = objectConfiguration.checks;	
		OclConstraintsCheck oclConstraintsCheck=null;
		for(Check check:checks){
			if(check instanceof OclConstraintsCheck) oclConstraintsCheck = (OclConstraintsCheck) check;
		}		
		if(oclConstraintsCheck==null){
			// setup oclConstraintsCheck
			oclConstraintsCheck = new OclConstraintsCheck();
			objectConfiguration.checks.add(oclConstraintsCheck);
		}
		
		// define oclConstraint
		OclConstraintCheck oclCheck = new OclConstraintCheck();
		oclCheck.setExpr(expr);
		oclCheck.setProfiles(profiles);
		oclCheck.setSeverity(severity);
		
		// append oclConstraint
		oclConstraintsCheck.checks.add(oclCheck);
	}
	
	private void streamTokenizer(File oclFile, String profiles, int severity) throws ClassNotFoundException, IOException{
		// create a new streamTokenizer
        Reader r = new BufferedReader(new FileReader(oclFile));
        StreamTokenizer st = new StreamTokenizer(r);

        // simple parse
        boolean eof = false;
        FSM fsm = new FSM();
        StringBuilder sb = new StringBuilder();
        String qualifiedPath = "";
        String context = "";
        
        do {
           int token = st.nextToken();
           switch (token) {
              case StreamTokenizer.TT_EOF:
                 eof = true;
                 break;
              case StreamTokenizer.TT_EOL:
                 break;
              case StreamTokenizer.TT_WORD:
            	  // switch between keywords
                  if(st.sval.equals("context")){
                	  if(fsm.getState()==STATE.PACKAGE){
                		  // fully qualified path
                		  qualifiedPath = sb.toString().replace("::", ".");
                	  }else if(fsm.getState()==STATE.CONTEXT){
                		  // create assert
                		  createAssert(sb.toString(), qualifiedPath+"."+context, severity, profiles);                		  
                	  }
                	  
            		  // start new context
            		  sb = new StringBuilder();
            		  sb.append(st.sval);
                		  
                  	  // get context name
                	  st.nextToken();
                	  context = st.sval;
                	  sb.append(" ");
                	  sb.append(context);
                	  
                	  // state transition
                	  fsm.setState(STATE.CONTEXT);
                  }else if(st.sval.equals("package")){
                	  st.nextToken();
                	  sb.append(st.sval);
                	  // state transition
                	  fsm.setState(STATE.PACKAGE);
                  }else if(st.sval.equals("endpackage")){
                 	 // create assert
                 	 createAssert(sb.toString(), qualifiedPath+"."+context, severity, profiles);
                  }  else{
                	  // expression body
                	  if(fsm.getState()!=STATE.PACKAGE) sb.append(" ");
                	  sb.append(st.sval);
                  }
                  break;
              case StreamTokenizer.TT_NUMBER:
            	  sb.append(st.nval);
            	  break;
              default:
                 if (token == '!') {
                    eof = true;
                 }else{
                	 // special chars
                	 sb.append((char)token);
                 }
           }
        } while (!eof);
	}
}