package org.zoodb.jdo.internal;

import java.io.IOException;

public class Serializer {

	public static void serializeSchema(Node n, ZooClassDef schema, 
			long oid, SerialOutput out) throws IOException {
		//write OID
		Session.assertOid(oid);
		out.writeLong(oid);
		
		//write class
		write(out, schema.getClassName());
		
		//write super class
		write(out, schema.getSuperClassName());

		//write fields
		ZooFieldDef[] fields = schema.getFields();
		out.writeInt(fields.length);
		
		for (ZooFieldDef f: fields) {
			//Name, type, isPC
			write(out, f.getName());
			write(out, f.getTypeName());
			out.writeBoolean(f.isPersistentType());
		}
//TODO?		out.flush();
	}
	
	
	public static ZooClassDef deSerializeSchema(Node node, SerialInput in, 
			ZooClassDef defSuper) throws IOException {
		//read OID
		long oid = in.readLong();
		
		//read class
		String className = readString(in);
		String sup = readString(in);
		
		//read fields
		int nF = in.readInt();
		String[] fNames = new String[nF];
		String[] tNames = new String[nF];
		boolean[] isPCs = new boolean[nF];
		
		for (int i = 0; i < nF; i++) {
			fNames[i] = readString(in);
			tNames[i] = readString(in);
			isPCs[i] = in.readBoolean();
		}
		
		Class<?> cls;
		try {
			cls = Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Class not found: \"" + className + "\"", e);
		}

		System.err.println("FIXME: Serializer.deSerializeSchema()");
		//TODO assert loading of super-schema
		//TODO check correctness of loaded schema
		
		//ISchema sch = node.createSchema(cls, oid, true);
		ZooClassDef sch = new ZooClassDef(cls, oid, defSuper);
		return sch;
	}
	
	
	public static void serializeUser(User user, SerialOutput out) throws IOException {
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
	
	
	public static User deSerializeUser(SerialInput in, Node node, int userID) throws IOException {
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
	
	
	private static String readString(SerialInput in) throws IOException {
		return in.readString();
	}


	private static final void write(SerialOutput out, String str) 
			throws IOException {
		out.writeString(str);
	}
}
