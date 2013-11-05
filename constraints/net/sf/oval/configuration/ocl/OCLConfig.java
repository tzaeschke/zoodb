/**
 * 
 */
package net.sf.oval.configuration.ocl;

import java.io.File;

/**
 * @author oserb
 * 
 */
public class OCLConfig {
	private int severity;
	private String profiles;
	private File oclFile;

	public OCLConfig(File oclFile, String profiles, int severity) {
		this.oclFile = oclFile;
		this.profiles = profiles;
		this.severity = severity;
	}

	public File getOclFile(){
		return oclFile;
	}
	
	public String getProfiles(){
		return profiles;
	}
	
	public int getSeverity(){
		return severity;
	}
}
