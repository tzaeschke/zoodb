/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.jdo.internal;

import java.util.zip.CRC32;

public class User {

    private String nameDB;
    private final String nameOS;
	private String password;
	private boolean isPasswordRequired;
	private boolean isDBA;
    private boolean isR;
    private boolean isW;
	private final int id;
	private long passwordCRC;
	
	public User(String nameOS, int id) {
		this.nameOS = nameOS;
		this.id = id;
	}
	
    public User(int id, String nameDB, String password, boolean isDBA, boolean isR,
            boolean isW, boolean hasPWD) {
        this.nameOS = System.getProperty("user.name");
        this.nameDB = nameOS;
        this.password = password;
        this.id = id;
        this.isDBA = isDBA;
        this.isR = isR;
        this.isW = isW;
        this.isPasswordRequired = hasPWD;
    }

    public String getNameOS() {
        return nameOS;
    }
    
    public String getNameDB() {
        return nameDB;
    }
    
    public void setNameDB(String nameDB) {
        this.nameDB = nameDB;
    }
    
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public boolean isPasswordRequired() {
		return isPasswordRequired;
	}
	
	public void setPasswordRequired(boolean isPasswordRequired) {
		this.isPasswordRequired = isPasswordRequired;
	}
	
	public boolean isDBA() {
		return isDBA;
	}
	
	public void setDBA(boolean isDBA) {
		this.isDBA = isDBA;
	}
	
    public boolean isR() {
        return isR;
    }
    
    public void setR(boolean isR) {
        this.isR = isR;
    }
    
    public boolean isW() {
        return isW;
    }
    
    public void setW(boolean isW) {
        this.isW = isW;
    }
    
	public int getID() {
	    return id;
	}

    public void setPassCRC(long passwordCRC) {
        this.passwordCRC = passwordCRC;
    }

    public long getPasswordCRC() {
        return passwordCRC;
    }

    public void calcPwdCRC() {
        CRC32 pwd = new CRC32();
        for (int i = 0; i < password.length(); i++) {
            pwd.update(password.charAt(i));
        }
        passwordCRC = pwd.getValue();
    }
}

