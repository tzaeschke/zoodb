package org.zoodb.jdo.doc;

import org.zoodb.api.impl.ZooPCImpl;

public class ExampleAddress extends ZooPCImpl {
	
	private String city;
	
	public ExampleAddress() {}
	
	public ExampleAddress(String city) {
		this.city = city;
	}

	public String getCity() {
		this.zooActivateRead();
		return city;
	}

	public void setCity(String city) {
		this.zooActivateWrite();
		this.city = city;
	}
	
	

}
