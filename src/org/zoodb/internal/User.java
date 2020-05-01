/*
 * Copyright 2009-2020 Tilmann Zaeschke
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zoodb.internal;

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

