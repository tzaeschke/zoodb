package org.zoodb.jdo.doc;

import org.zoodb.api.impl.ZooPCImpl;

public class ExampleAddress extends ZooPCImpl {
	
	private ExampleCity city;
	private String dummyName;
	
	public ExampleAddress() {}
	
	public ExampleAddress(ExampleCity city) {
		this.city = city;
	}

	public ExampleCity getCity() {
		this.zooActivateRead();
		return city;
	}

	public void setCity(ExampleCity city) {
		this.zooActivateWrite();
		this.city = city;
	}

	public String getDummyName() {
		this.zooActivateRead();
		return dummyName;
	}

	public void setDummyName(String dummyName) {
		this.zooActivateWrite();
		this.dummyName = dummyName;
	}

}
