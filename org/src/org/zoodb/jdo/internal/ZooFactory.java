package org.zoodb.jdo.internal;

import org.zoodb.jdo.internal.client.session.ClientSessionCache;


public abstract class ZooFactory {

	private static ZooFactory f;
	
	public static ZooFactory get() {
		if (f == null) {
			try {
				f = (ZooFactory) findClass("Factory").newInstance();
			} catch (InstantiationException e) {
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} 
		}
		return f;
	}
	
	private static Class<?> findClass(String clsName) {
		try {
			String root = "org.zoodb.jdo.internal.model";
			if (Config.MODEL == Config.MODEL_1P) {
				return Class.forName(root + "1p." + clsName + "1P");
			} else if (Config.MODEL == Config.MODEL_2P) {
				return Class.forName(root + "2p." + clsName + "2P"); 
			}
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} 
		throw new IllegalStateException("Model = " + Config.MODEL);
	}
	
	public abstract Node createNode(String nodePath, ClientSessionCache cache);

}
