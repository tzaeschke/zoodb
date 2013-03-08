package org.zoodb.profiler.test.test_071;

import org.zoodb.jdo.spi.PersistenceCapableImpl;
import org.zoodb.profiler.test.test_071.Department;
import org.zoodb.profiler.test.test_071.Employee;

public class Employee extends PersistenceCapableImpl {
	String name;
	float salary;
	Department dept;
	Employee boss;
	private Employee () {}  //TODO remove later, once BCE is in place.
	public Employee(String aName, float aSalary, Department aDept, Employee aBoss) {
		name = aName;
		salary = aSalary;
		dept = aDept;
		boss = aBoss;
	}
	public float getSalary() {
		return salary;
	}
}
