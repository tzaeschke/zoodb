package org.zoodb.test.test_071;

import org.zoodb.jdo.spi.PersistenceCapableImpl;

public class Employee extends PersistenceCapableImpl {
	String name;
	float salary;
	Department dept;
	Employee boss;
	int weeklyhours;
	
	@SuppressWarnings("unused")
	private Employee () {
		//TODO remove later, once BCE is in place.
	} 

	public Employee(String aName, float aSalary, Department aDept, Employee aBoss) {
		name = aName;
		salary = aSalary;
		dept = aDept;
		boss = aBoss;
	}

	public String getName() {
		return name;
	}

	public float getSalary() {
		return salary;
	}
}
