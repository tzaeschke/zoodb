package ch.ethz.oserb.exception;

import java.util.List;

import ch.ethz.oserb.violation.Violation;

public class ConstraintException extends Exception {
	
	private List<Violation> violations;
	
	public ConstraintException(List<Violation> violations) {
		this.violations = violations;
	}
	
	public List<Violation> getViolations(){
		return violations;
	}

}
