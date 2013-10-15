package ch.ethz.oserb.violation;

import java.util.List;

public class Violation {

	public enum Severity {
		INFO(0), WARNING(1), ERROR(3), IGNORE(4);
		private int value;

		private Severity(int value) {
			this.value = value;
		}
	}
	
	private Severity severity;
	private String constraint;
	
	public Violation(String constraint, Severity severity) {
		this.constraint = constraint;
		this.severity = severity;
	}
	
	public Severity getSeverity(){
		return severity;
	}
	
	public String getConstraint(){
		return constraint;
	}
}
