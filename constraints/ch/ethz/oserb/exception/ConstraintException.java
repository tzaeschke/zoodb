package ch.ethz.oserb.exception;

import java.util.List;

import ch.ethz.oserb.ConstraintManager;
import net.sf.oval.ConstraintViolation;
import net.sf.oval.exception.ConstraintsViolatedException;

public class ConstraintException extends ConstraintsViolatedException {
	
	private ConstraintManager cm;
	/**
	 * 
	 */
	private static final long serialVersionUID = 985927737601708018L;
	
	public ConstraintException(ConstraintManager cm){
		this.cm = cm;
	}
}
