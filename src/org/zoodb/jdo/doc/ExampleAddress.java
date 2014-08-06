package org.zoodb.jdo.doc;

import org.zoodb.jdo.spi.PersistenceCapableImpl;


public class ExampleAddress extends PersistenceCapableImpl {
	
	private ExampleCity city;
	private String dummyName;
	
	public ExampleAddress() {}
	
	public ExampleAddress(ExampleCity city) {
		this.city = city;
	}

	public ExampleCity getCity() {
		activateRead("city");
		return city;
	}

	public void setCity(ExampleCity city) {
		activateWrite("city");
		this.city = city;
	}

	public String getDummyName() {
		activateRead("dummyName");
		return dummyName;
	}

	public void setDummyName(String dummyName) {
		activateWrite("dummyName");
		this.dummyName = dummyName;
	}

}