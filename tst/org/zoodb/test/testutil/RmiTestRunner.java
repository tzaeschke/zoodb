/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
 * 
 * This file is part of ZooDB.
 * 
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * See the README and COPYING files for further information. 
 */
package org.zoodb.test.testutil;

import java.rmi.RMISecurityManager;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.Permission;

class RmiTestRunner implements RmiTestRunnerAPI {

	static class NoSecurityManager extends RMISecurityManager {
	    public void checkConnect (String host, int port) {}
	    public void checkConnect (String host, int port, Object context) {}
	    public void checkPropertyAccess(String key) {};
	    public void checkPermission(Permission perm) {};
	    public void checkPermission(Permission perm, Object context) {};
	    public void checkAccept(String host, int port) {};   
	}
	
	public void executeTask(RmiTestTask task) {
		//perform the actual task
        task.test();
    }
	
	public static void main(String[] args) {
//		System.out.println("RmiTestRunner started: " + args[0]);
//		System.out.println("RmiTestRunner time: " + new Date());
		
		System.setSecurityManager (new NoSecurityManager());
        try {
            RmiTestRunner engine = new RmiTestRunner();
            RmiTestRunnerAPI stub =
                (RmiTestRunnerAPI) UnicastRemoteObject.exportObject(engine, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind(RmiTaskLauncher.RMI_NAME, stub);
//            System.out.println("RmiTestRunner bound");
        } catch (Exception e) {
 //           System.err.println("RmiTestRunner exception:");
            e.printStackTrace();
        }
		
//		System.out.println("RmiTestRunner finished: " + args[0]);
	}

	
}
