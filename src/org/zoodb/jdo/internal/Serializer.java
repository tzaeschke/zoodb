/*
 * Copyright 2009-2011 Tilmann Zäschke. All rights reserved.
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

import java.util.ArrayList;

import javax.jdo.JDOFatalDataStoreException;

import org.zoodb.jdo.internal.ZooFieldDef.JdoType;
import org.zoodb.jdo.internal.server.PageAccessFile;


public class Serializer {

	public static void serializeSchema(ZooClassDef schema, 
			long oid, SerialOutput out) {
		//write OID
		Session.assertOid(oid);
		out.writeLong(oid);
		
		//write class
		write(out, schema.getClassName());
		
		//write super class
		out.writeLong(schema.getSuperOID());

		//write fields
		ArrayList<ZooFieldDef> fields = schema.getLocalFields();
		out.writeInt(fields.size());
		
		for (ZooFieldDef f: fields) {
			out.writeLong(f.getTypeOID());
			//Name, type, isPC
			write(out, f.getName());
			write(out, f.getTypeName());
			out.writeShort((short) f.getOffset());
			out.writeByte((byte)f.getJdoType().ordinal());
			//TODO store in BitMap
//			out.writeBoolean(f.isPersistentType());
//			out.writeBoolean(f.isPrimitiveType());
//			out.writeBoolean(f.isArray());
//			out.writeBoolean(f.isString());
		}
	}
	
	
	public static ZooClassDef deSerializeSchema(SerialInput in) {
		//read OID
		long sOid = in.readLong();
		
		//read class
		String className = readString(in);
		long supOid = in.readLong();
		
        ZooClassDef sch = ZooClassDef.createFromDatabase(className, sOid, supOid);

        //read fields
		int nF = in.readInt();
		ArrayList<ZooFieldDef> fields = new ArrayList<ZooFieldDef>();
		
		for (int i = 0; i < nF; i++) {
			long oid = in.readLong();
			String name = readString(in);
			String tName = readString(in);
			short ofs = in.readShort();
			JdoType jdoType = JdoType.values()[in.readByte()]; 

			ZooFieldDef f = new ZooFieldDef(sch, name, tName, jdoType);
			f.setOffset(ofs);
			fields.add(f);
		}
		
		sch.addFields(fields);
		return sch;
	}
	

	public static void deSerializeSchema(PageAccessFile in, ZooClassDef def) {
		//read OID
		long sOid = in.readLong();
		
		//read class
		String className = readString(in);
		long supOid = in.readLong();
		
		if (sOid != def.getOid()) {
			throw new JDOFatalDataStoreException();
		}
		if (!className.equals(def.getClassName())) {
			throw new JDOFatalDataStoreException();
		}
		if (supOid != def.getSuperOID()) {
			throw new JDOFatalDataStoreException();
		}
        ZooClassDef sch = def;//new ZooClassDef(className, sOid, supOid);

        //read fields
		int nF = in.readInt();
		ArrayList<ZooFieldDef> fields = new ArrayList<ZooFieldDef>();
		
		for (int i = 0; i < nF; i++) {
			long oid = in.readLong();
			String name = readString(in);
			String tName = readString(in);
			short ofs = in.readShort();
			JdoType jdoType = JdoType.values()[in.readByte()]; 

			ZooFieldDef f = new ZooFieldDef(sch, name, tName, jdoType);
			f.setOffset(ofs);
			fields.add(f);
		}
		
		sch.addFields(fields);
	}

	
	public static void serializeUser(User user, SerialOutput out) {
	    out.writeInt(user.getID());
	    
        //write flags
        out.writeBoolean(user.isDBA());// DBA=yes
        out.writeBoolean(user.isR());// read access=yes
        out.writeBoolean(user.isW());// write access=yes
        out.writeBoolean(user.isPasswordRequired());// passwd=no
        
        //write name
        out.writeString(user.getNameDB());
        
        //use CRC32 as basic password encryption to avoid password showing up in clear text.
        out.writeLong(user.getPasswordCRC());
	}
	
	
	public static User deSerializeUser(SerialInput in, Node node, int userID) {
        String uNameOS = System.getProperty("user.name");
        User user = new User(uNameOS, userID);
        
        //read flags
        user.setDBA( in.readBoolean() );// DBA=yes
        user.setR( in.readBoolean() );// read access=yes
        user.setW( in.readBoolean() );// write access=yes
        user.setPasswordRequired( in.readBoolean() );// passwd=no
        
        //read name
        user.setNameDB( in.readString() );
        
        //use CRC32 as basic password encryption to avoid password showing up in clear text.  
        long uPassWordCRC = in.readLong(); //password CRC32
        user.setPassCRC(uPassWordCRC);
		
		return user;
	}
	
	
	private static String readString(SerialInput in) {
		return in.readString();
	}


	private static final void write(SerialOutput out, String str) {
		out.writeString(str);
	}
}
