package org.zoodb.profiling.model2;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class PublicationAbstract extends PersistenceCapableImpl {
	
	private String Abstract;

	public PublicationAbstract() {}

	public String getAbstract() {
		activateRead("Abstract");
		return Abstract;
	}

	public void setAbstract(String Abstract) {
		activateWrite("Abstract");
		this.Abstract = Abstract;
	}
	
	
	
	
	

}
