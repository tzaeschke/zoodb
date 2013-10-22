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
	
	public OCLConfigurer(File oclFile, IModel model) throws FileNotFoundException, IOException{
		LOG.info("OCL configurer registered: {1}", this.getClass().getName());
		this.oclFile = oclFile;
		this.model = model;
		
        // create an ObjectInputStream
        //ObjectInputStream ois = new ObjectInputStream(new FileInputStream(oclFile));

        // create a new tokenizer
        Reader r = new BufferedReader(new FileReader(oclFile));
        StreamTokenizer st = new StreamTokenizer(r);

        // print the stream tokens
        boolean eof = false;
        FSM fsm = new FSM();
        StringBuilder sb = new StringBuilder();
        
        do {
           int token = st.nextToken();
           switch (token) {
              case StreamTokenizer.TT_EOF:
                 eof = true;
                 break;
              case StreamTokenizer.TT_EOL:
                 break;
              case StreamTokenizer.TT_WORD:
                  if(st.sval.equals("context")){
                	  // state transition
                	  fsm.setState(FSM.STATE.COLLECTING);
                 	 
	              	   switch (fsm.getState()) {
	              	   case COLLECTING:
	                  	 // create assert
	                  	 createAssert(sb.toString());
	              		   
	              		  //fall-through
	              	   	case INITIAL:
	              	   		// start new context
	              	   		sb = new StringBuilder();
	              	   		sb.append(st.sval);
	              	   			break;
	              	   }
                  }else if(st.sval.equals("endpackage")){
                	 // create assert
                	 createAssert(sb.toString());
                  }else{
                	  // expression body
	              	   switch (fsm.getState()) {
	              	   	case INITIAL:
	              	   		// ignore & skip
	              	   			break;
	              	   	case COLLECTING:
	              	   		// append to context
	              	   		sb.append(" ");
	              	   		sb.append(st.sval);
	              	   		break;
	              	   }
                  }
                  break;
              case StreamTokenizer.TT_NUMBER:
             	   switch (fsm.getState()) {
             	   	case INITIAL:
             	   		// ignore & skip
             	   			break;
             	   	case COLLECTING:
             	   		// append to context
             	   		sb.append(st.nval);
             	   		break;
             	   }
            	  break;
              default:
                 if (token == '!') {
                    eof = true;
                 }else{
               	   switch (fsm.getState()) {
            	   	case INITIAL:
            	   		// ignore & skip
            	   			break;
            	   	case COLLECTING:
            	   		// append to context
            	   		sb.append((char) token);
            	   		break;
            	   }
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
	
	private void createAssert(String expr){
		Set<ClassConfiguration> classConfigurations = new HashSet<ClassConfiguration>();
		pojoConfigurer.setClassConfigurations(classConfigurations);
		
		// define assert expression
		AssertCheck assertCheck = new AssertCheck();
		assertCheck.setExpr(expr);
		assertCheck.setLang("ocl");
		
		// define class configuration			
		ClassConfiguration classConfiguration = new ClassConfiguration();
		classConfigurations.add(classConfiguration);
		// TODO:
		classConfiguration.type = ExamplePerson.class;
		
		// define object configuration
		ObjectConfiguration objectConfiguration = new ObjectConfiguration();
		classConfiguration.objectConfiguration = objectConfiguration;
		objectConfiguration.checks = new ArrayList<Check>();
		objectConfiguration.checks.add(assertCheck);
	}
}
