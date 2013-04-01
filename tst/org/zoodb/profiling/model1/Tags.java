package org.zoodb.profiling.model1;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class Tags extends PersistenceCapableImpl     {	

	private String label;

	private List<Publication> sourceT;
	
	
	public Tags() {
		sourceT = new LinkedList<Publication>();
	}
	
	/**
	 * Getter method of the reverse direction of the association sourceT.
	 * @return the List<Publication>.
	 */
	public List<Publication> getSourceT() {
		activateRead("sourceT");
	  	return this.sourceT;
	}	
	
	/**
	 * Setter method of the reverse direction of the association sourceT.
	 * @param SourceT.
	 */
	public void setSourceT(List<Publication> sourceT) {
		activateWrite("sourceT");
	  	this.sourceT = sourceT;
	}
	
	
	public void addPublication(Publication p) {
		activateWrite("sourceT");
		sourceT.add(p);
	}


	/**
     * Gets label.
     * @return value of label.
	 */
    public String getLabel() {
		activateRead("label");
	  	return this.label;
	}
		
	/**
     * Sets label.
     * @param label New value of label.
	 */
    public void setLabel(String label) {
		activateWrite("label");
		this.label = label;
	}






    @Override
    public int hashCode() {
        // Start of user code for Tags.hashCode()
        int hash = 7;
        hash = 31 * hash + (null == this.label ? 0 : this.label.hashCode());
        return hash;
        // End of user code
    }

    @Override
    public boolean equals(Object obj) {
        // Start of user code for Tags equality
        if (this == obj) {
            return true;
        }
        if (obj instanceof Tags) {
            Tags myO = (Tags) obj;
            boolean isEqual = true;
            
            isEqual &= this.label == myO.label || (this.label != null && this.label.equals(myO.label));
            return isEqual;
        }
        return false;		
        // End of user code	
    }

	
	// Start of user code for Tags
	// End of user code	
}
