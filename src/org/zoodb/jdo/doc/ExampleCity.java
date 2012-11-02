package org.zoodb.jdo.doc;

import org.zoodb.jdo.spi.PersistenceCapableImpl;


public class ExampleCity extends PersistenceCapableImpl {
	
	private String name;
	
	public ExampleCity() {}
	
	public ExampleCity(String name) {
		this.name = name;
	}

	public String getName() {
		activateRead("name");
		return name;
	}

	public void setName(String name) {
		activateWrite("name");
		this.name = name;
	}
	
}