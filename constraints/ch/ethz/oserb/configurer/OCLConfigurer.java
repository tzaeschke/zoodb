/**
 * 
 */
package ch.ethz.oserb.configurer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StreamTokenizer;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.ethz.oserb.configurer.FSM.STATE;
import ch.ethz.oserb.example.ExamplePerson;
import tudresden.ocl20.pivot.interpreter.IInterpretationResult;
import tudresden.ocl20.pivot.model.IModel;
import tudresden.ocl20.pivot.modelinstance.IModelInstance;
import tudresden.ocl20.pivot.modelinstancetype.java.internal.modelinstance.JavaModelInstance;
import tudresden.ocl20.pivot.parser.ParseException;
import tudresden.ocl20.pivot.pivotmodel.Constraint;
import tudresden.ocl20.pivot.standalone.facade.StandaloneFacade;
import tudresden.ocl20.pivot.standardlibrary.java.internal.library.JavaOclBoolean;
import net.sf.oval.Check;
import net.sf.oval.Validator;
import net.sf.oval.configuration.Configurer;
import net.sf.oval.configuration.pojo.POJOConfigurer;
import net.sf.oval.configuration.pojo.elements.ClassConfiguration;
import net.sf.oval.configuration.pojo.elements.ConstraintSetConfiguration;
import net.sf.oval.configuration.pojo.elements.MethodConfiguration;
import net.sf.oval.configuration.pojo.elements.ObjectConfiguration;
import net.sf.oval.constraint.Assert;
import net.sf.oval.constraint.AssertCheck;
import net.sf.oval.exception.InvalidConfigurationException;
import net.sf.oval.internal.Log;

/**
 * @author oserb
 *
 */
public class OCLConfigurer implements Configurer, Serializable {
	
	private IModel model;
	private IModelInstance modelInstance;
	private File oclFile;
	private List<Constraint> constraintList;
	private POJOConfigurer pojoConfigurer = new POJOConfigurer();
	private static final Log LOG = Log.getLog(Validator.class);
	
	public OCLConfigurer(File oclFile, IModel model, int severity, String profiles) throws FileNotFoundException, IOException, ClassNotFoundException{
		LOG.info("OCL configurer registered: {1}", this.getClass().getName());
		this.oclFile = oclFile;
		this.model = model;
		        
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
		Set<ClassConfiguration> classConfigurations = new HashSet<ClassConfiguration>();
		pojoConfigurer.setClassConfigurations(classConfigurations);
		
		// define assert expression
		AssertCheck assertCheck = new AssertCheck();
		assertCheck.setExpr(expr);
		assertCheck.setLang("ocl");
		assertCheck.setProfiles(profiles);
		assertCheck.setSeverity(severity);
		
		// define class configuration			
		ClassConfiguration classConfiguration = new ClassConfiguration();
		classConfigurations.add(classConfiguration);
		classConfiguration.type = Class.forName(context);

		
		// define object configuration
		ObjectConfiguration objectConfiguration = new ObjectConfiguration();
		classConfiguration.objectConfiguration = objectConfiguration;
		objectConfiguration.checks = new ArrayList<Check>();
		objectConfiguration.checks.add(assertCheck);
	}
}
