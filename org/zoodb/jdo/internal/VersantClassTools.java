/*
 * $Id: VersantClassTools.java,v 1.4 2008/03/18 12:13:32 tzaeschk Exp $
 *
 * Copyright (c) 2002 European Space Agency
 */
package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * This class contains utility methods that need to be embedded in Versant 
 * enhanced code. 
 *
 * @author Tilmann Zaeschke
 */
class VersantClassTools {

    // Synchronised to allow concurrent access from different Threads.
    private static final Map<Class<?>, List<Field>> _seenClasses = 
        Collections.synchronizedMap(new HashMap<Class<?>, List<Field>>());

    static final List<Field> getFields(Class<?> cl) {
        List<Field> fields = _seenClasses.get(cl);
        if (fields != null) {
            return _seenClasses.get(cl);
        }

        // TODO the following could be optimised:
        // Instead of filling a List with all attributes and then removing the
        // undesired ones, the List should be build up only with desired
        // attributes
        // while looping through the individual arrays from getDeclaredFields();

        // Add attributes of super-classes for jar,
        // these are already included in the database class.
        Field[] fieldA = cl.getDeclaredFields();
        fields = new LinkedList<Field>(Arrays.asList(fieldA));
        Class<?> cSuper = cl.getSuperclass();
        while (cSuper != null) {
            Collections.addAll(fields, cSuper.getDeclaredFields());
            cSuper = cSuper.getSuperclass();
        }

        // Remove transient and static attrs from list (they are not in the DB)
        ListIterator<Field> vil = fields.listIterator();
        vil = fields.listIterator(0);
        int modifiers;
        while (vil.hasNext()) {
            try {
                Field field = vil.next();
                modifiers = field.getModifiers();
                if (Modifier.isTransient(modifiers)
                        || Modifier.isStatic(modifiers)) {
                    vil.remove();
                    continue;
                }

                if (field.getName().startsWith("_vj_")) {
                    throw new IllegalStateException("Bad field: " + field);
                }

                // The following is alright because this class is only used
                // in a daemon.
                field.setAccessible(true);
            } catch (RuntimeException e) {
                throw e;
            }
        }
        //Because we are not in a synchronised block, this may actually
        //overwrite an existing entry. But it's very unlikely and would not do
        //any harm, so we do not synchronise here. Using a synchronised Map
        //should be sufficient.
        _seenClasses.put(cl, fields);

        return fields;
    }

    /**
     * This method ensures that the object is available in the java cache.
     * @param obj
     */
    static void loadObject(Object obj) {
        try {
            obj.hashCode();
        } catch (NullPointerException e) {
            //TODO see SPR 3809        obj.hashCode();
            //E.g. InstrumentModels used to occasionally throw NPEs.
            //It doesn't really matter, though, 
        }
    }
}
