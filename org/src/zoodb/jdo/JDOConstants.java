/*
 * $Id: JDOConstants.java,v 1.10 2007/09/10 09:06:37 tzaeschk Exp $
 *
 * Copyright (c) 2002 European Space Agency
 */
package org.zoodb.jdo;

/**
 * This class contains constant definitions of JDO constants. This class 
 * is not part of the JDO standard.
 * 
 * @author Tilmann Zaeschke
 */
public abstract class JDOConstants {

    /** <code>OPTION_USERNAME</code> = "userName"*/
    public static final String OPTION_USERNAME = "userName";
    /** <code>OPTION_PASSWORD</code> = "userPassword"*/
    public static final String OPTION_PASSWORD = "userPassword";
    
    /** <code>VERSANT_DATABASE_NAME</code> = "database"*/
    public static final String VERSANT_DATABASE_NAME = "database";

    /**
     * Database root name for the object that associates the shadow
     * database with a database system.
     */
    public static final String ROOT_NAME_DB_SYSTEM_FOR_SHADOW = 
        "DB_SYSTEM_FOR_SHADOW";
    /**
     * Database root name for the registry of changed objects in the shadow
     * database.
     */
    public static final String ROOT_NAME_CHANGE_REGISTRY_FOR_SHADOW = 
        "CHANGE_REGISTRY_FOR_SHADOW";
}
