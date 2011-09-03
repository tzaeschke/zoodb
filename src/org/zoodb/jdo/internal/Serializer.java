package org.zoodb.jdo.internal;

import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.internal.ZooFieldDef.JdoType;


public class Serializer {

	public static void serializeSchema(Node n, ZooClassDef schema, 
			long oid, SerialOutput out) {
		//write OID
		Session.assertOid(oid);
		out.writeLong(oid);
		
		//write class
		write(out, schema.getClassName());
		
		//write super class
		out.writeLong(schema.getSuperOID());

		//write fields
		List<ZooFieldDef> fields = schema.getLocalFields();
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
	
	
	public static ZooClassDef deSerializeSchema(Node node, SerialInput in) {
		//read OID
		long sOid = in.readLong();
		
		//read class
		String className = readString(in);
		long supOid = in.readLong();
		
        ZooClassDef sch = new ZooClassDef(className, sOid, supOid);

        //read fields
		int nF = in.readInt();
		List<ZooFieldDef> fields = new LinkedList<ZooFieldDef>();
		
		for (int i = 0; i < nF; i++) {
			long oid = in.readLong();
			String name = readString(in);
			String tName = readString(in);
			short ofs = in.readShort();
			JdoType jdoType = JdoType.values()[in.readByte()]; 

			ZooFieldDef f = new ZooFieldDef(sch, name, tName, oid, jdoType);
			f.setOffset(ofs);
			fields.add(f);
		}
		
		sch.addFields(fields);
		return sch;
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
