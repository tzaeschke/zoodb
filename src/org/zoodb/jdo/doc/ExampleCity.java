package org.zoodb.jdo.doc;

import org.zoodb.api.impl.ZooPCImpl;

public class ExampleCity extends ZooPCImpl {
	
	private String name;
	
	public ExampleCity() {
		
	}
	
	public ExampleCity(String name) {
		this.name = name;
	}

	public String getName() {
		zooActivateRead();
		return name;
	}

	public void setName(String name) {
		zooActivateRead();
		this.name = name;
	}
	
	

}
