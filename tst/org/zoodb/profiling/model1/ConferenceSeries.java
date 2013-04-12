package org.zoodb.profiling.model1;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class ConferenceSeries extends PersistenceCapableImpl     {	

	private String DBLPkey;
	
	private String name;

	//private final List<Conference> conferences;
	private final List<Publication> publications;
    
    public ConferenceSeries() {
    	//conferences = new LinkedList<Conference>();
    	publications = new LinkedList<Publication>();
    }
    
    
	/**
	 * the Association conferences getter.
	 * @return the association conferences.
	 */
	public List<Conference> getConferences() {
		activateRead("conferences");
		throw new UnsupportedOperationException();
	  	//return this.conferences;
	}
		
//	/**
//	 * the Association conferences setter.<br>
//	 * pre: conferences <> null  and conferences -> size() >= 1 <br>
//	 * post: self.conferences = conferences<br>
//	 *
//	 * @param conferences the association conferences to set.
//	 * @throws Exception	 
//	 */
//	public void setConferences(List<Conference> conferences) throws Exception{
////		activateWrite("conferences");
////		if(conferences != null && conferences.size() < 1){
////			throw new Exception("Constraint violation: conferences must have size of at least 1");
////		}
////		this.conferences = conferences;
//	}
	
	/**
	 * the Association conferences collection method.<br>
	 * pre: conference <> null and self.conferences <> null <br>
	 * post: self.conferences -> size() = self.conferences@pre -> size() + 1 <br>
	 *
	 * @param conference a reference to association end of Conference
	 * @throws Exception	 
	 */
	public void addConferences(Conference conference) {
//		activateWrite("conferences");
//		if (this.conferences == null) {
//			throw new IllegalStateException("Association conferences is not initialized yet. " +
//					"Please use setConferences(List<Conference>) instead");
//		}
//		if(conference != null){
//			this.conferences.add(conference);
//		}		
		publications.addAll(conference.getPublications());
	}

    public String getName() {
		activateRead("name");
	  	return this.name;
	}
		
    public void setName(String name) {
		activateWrite("name");
		this.name = name;
	}
    
    public String getDBLPkey() {
    	activateRead("DBLPkey");
		return DBLPkey;
	}

	public void setDBLPkey(String dBLPkey) {
		activateWrite("DBLPkey");
		DBLPkey = dBLPkey;
	}


	@Override
    public int hashCode() {
        // Start of user code for ConferenceSeries.hashCode()
        int hash = 7;
        hash = 31 * hash + (null == this.name ? 0 : this.name.hashCode());
        return hash;
        // End of user code
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ConferenceSeries) {
            ConferenceSeries myO = (ConferenceSeries) obj;
            boolean isEqual = true;
            
            isEqual &= this.name == myO.name || (this.name != null && this.name.equals(myO.name));
            return isEqual;
        }
        return false;		
 
    }


	public List<Publication> getPublications() {
		return publications;
	}
}