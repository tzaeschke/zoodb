package org.zoodb.test.jdo.pole;

public interface Visitor<T> {

	void visit(T holder);

}
