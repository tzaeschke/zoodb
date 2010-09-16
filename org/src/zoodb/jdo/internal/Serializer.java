package org.zoodb.jdo.internal;

import java.io.IOException;

public class Serializer {

	public static void serializeSchema(Node n, ZooClassDef schema, 
			long oid, SerialOutput out) throws IOException {
		Class<?> cls = schema.getSchemaClass();
//		Class<?> sup = cls.getSuperclass();
//		Field[] fields = cls.getDeclaredFields();

		//write OID
		Session.assertOid(oid);
		out.writeLong(oid);
		
		//write class
		write(out, schema.getClassName());
		
		//write super class
		write(out, schema.getSuperClassName());
//		if (sup != PersistenceCapableImpl.class && !n.isSchemaDefined(sup)) {
//			if (cls != DBHashtable.class && cls != DBVector.class) {
//				throw new JDOUserException("Class: " + cls.getName() + " -> " +
//						"Super-Class not persistent capable: " + sup.getName());
//			}
//		}
		

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

		//write name
		write(out, user.getName());
		
		//write password
		write(out, user.getPassword());
		
		//write flags
		out.writeBoolean(user.isDBA());
		out.writeBoolean(user.isPasswordRequired());
		out.writeBoolean(user.isRW());
	}
	
	
	public static User deSerializeUser(SerialInput in, String node) throws IOException {
		//read name
		String name = readString(in);
		User user = new User(name);
		
		//read password
		String password = readString(in);
		user.setPassword(password);
		
		//read flags
		user.setDBA(in.readBoolean());
		user.setPasswordRequired(in.readBoolean());
		user.setRW(in.readBoolean());
		
		return user;
	}
	
	
	private static String readString(SerialInput in) throws IOException {
		int l = in.readInt();
		char[] chars = new char[l];
		for (int i = 0; i < l; i++) {
			chars[i] = in.readChar();	
		}
		return String.valueOf(chars);
	}


	private static final void write(SerialOutput out, String str) 
			throws IOException {
		out.writeInt(str.length());
		out.writeChars(str);
	}
}
