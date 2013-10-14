package ch.ethz.oserb.exception;

public class ConstraintException extends Exception {

	public enum Severity {
		INFO(0), WARNING(1), ERROR(3);
		private int value;

		private Severity(int value) {
			this.value = value;
		}
	}
	
	public Severity severity;
	public String msg;

	public ConstraintException() {

	}
	
	public ConstraintException(Severity severity) {

	}
}
