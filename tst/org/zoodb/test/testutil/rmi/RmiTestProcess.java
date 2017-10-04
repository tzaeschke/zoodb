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
package org.zoodb.test.testutil.rmi;

import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.zoodb.internal.util.DBLogger;
import org.zoodb.test.testutil.rmi.RmiTaskRunner.NoSecurityManager;

public class RmiTestProcess implements RmiTestProcessI {

	@Override
	public void executeTask(RmiTestTask task) {
		try {
			task.test();
		} finally {
			try {
				UnicastRemoteObject.unexportObject(this, true);
			} catch (NoSuchObjectException e) {
				throw DBLogger.wrap(e);
			}
		}
    }
	
	@Override
	public boolean isAlive() {
		return true;
	}
	
	public static void main(String[] args) {
		//System.out.println("TestRunnerLocal: started: " + args[0] + " " + args[1] + " " + args[2]);
		//System.out.println("TestRunnerLocal time: " + new Date());
		
		System.setSecurityManager (new NoSecurityManager());
        try {
        	RmiTestProcess engine = new RmiTestProcess();
            RmiTestProcessI stub =
                (RmiTestProcessI) UnicastRemoteObject.exportObject(engine, 0);
            Registry registry = LocateRegistry.getRegistry();
            System.err.println("TestRunnerLocal bound");
            registry.rebind(RmiTaskRunner.RMI_NAME, stub);
            System.err.println("TestRunnerLocal bound");
        } catch (Exception e) {
            System.err.println("TestRunnerLocal exception:");
            throw DBLogger.wrap(e);
        }
		
		//System.out.println("TestRunnerLocal finished: " + args[0]);
	}

}
