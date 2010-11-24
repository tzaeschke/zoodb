package org.zoodb.test.test_071;

import java.util.Collection;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class Department extends PersistenceCapableImpl {
	String name;
	Collection<Employee> emps;
	
	private Department() {
		// for JDO
	}

	
	public Department(String name) {
		this.name = name;
	}
}
