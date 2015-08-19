/**
 * VMware Continuent Tungsten Replicator
 * Copyright (C) 2015 VMware, Inc. All rights reserved.
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
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): Gilles Rayrat
 */

package com.continuent.tungsten.common.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonAnyGetter;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Defines a simple HashMap wrapper that can be used to store and retrieve
 * properties using typed getters and setters. There is support for serializing
 * to and from Java properties format, setting variables, merging properties,
 * and other niceties.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class TungstenProperties implements Serializable
{
    private static final long   serialVersionUID    = 1;
    private static Logger       logger              = Logger.getLogger(TungstenProperties.class);
    public static final String  ENDOFPROPERTIES_TAG = "#EOF";
    public static final String  ENDOFLINE_TAG       = "#EOL";
    private static final String MAP_KEY_SEPARATOR   = "#TP_KEY#";

    enum ParseState
    {
        NONE, DOLLAR, LBRACKET, NAME
    };

    @JsonIgnore
    protected Map<String, Object> properties;
    private boolean               sorted;
    @JsonIgnore
    private boolean               beanSupportEnabled = false;

    // --- Support for "flat" JSON ---
    /*
     * This allows the Jackson JSON serialisation to "flatten" the properties
     * with the rest of the atributes
     */
    // "any getter" needed for serialization
    @JsonAnyGetter
    public Map<String, Object> any()
    {
        return properties;
    }

    @JsonAnySetter
    public void set(String name, Object value)
    {
        properties.put(name, value);
    }

    // ------------------------------

    /**
     * Creates a new instance.
     */
    public TungstenProperties()
    {
        this(false);
    }

    /**
     * Creates a new instance from an existing map.
     */
    public TungstenProperties(Map<String, String> map)
    {
        properties = new HashMap<String, Object>(map);
    }

    /**
     * Creates a new instance with sorting.
     */
    public TungstenProperties(boolean sorted)
    {
        properties = new HashMap<String, Object>();
        this.sorted = sorted;
    }

    /**
     * Returns true if this instance supports extracting/setting bean
     * properties.
     */
    public boolean isBeanSupportEnabled()
    {
        return beanSupportEnabled;
    }

    /**
     * If set to true this instance will support extracting/setting bean
     * properties.
     * 
     * @param beanSupportEnabled
     */
    public void setBeanSupportEnabled(boolean beanSupportEnabled)
    {
        this.beanSupportEnabled = beanSupportEnabled;
    }

    /**
     * Loads values from Java properties file format with variable
     * substitutions.
     */
    public void load(InputStream is) throws IOException
    {
        load(is, true);
    }

    /**
     * Loads values from Java properties file format. Current values are
     * obliterated.
     * 
     * @param is InputStream containing properties.
     * @param doSubstitutions If true perform variable substitutions
     */
    public void load(InputStream is, boolean doSubstitutions)
            throws IOException
    {
        // Load the properties file.
        Properties props = new Properties();
        props.load(is);
        if (doSubstitutions)
            substituteSystemValues(props);
        load(props);

        /*
         * props ends up holding a reference to the InputStream. So even
         * though the Properties instance should be eligible for collection on
         * method exit, give GC a hand and null out the props here.
         */
        props = null;
    }

    /**
     * Loads values from a string of name-value pairs of the form
     * a=1;b=2;...;z=N. White space is ignored.
     */
    public void load(String nameValuePairs, boolean doSubstitutions)
    {
        Properties props = new Properties();
        boolean parsingKey = true;
        int index = 0;
        StringBuffer keyBuf = new StringBuffer();
        StringBuffer valueBuf = new StringBuffer();
        while (index < nameValuePairs.length())
        {
            char next = nameValuePairs.charAt(index++);
            if (parsingKey && next == '=')
            {
                parsingKey = false;
            }
            else if (parsingKey)
            {
                keyBuf.append(next);
            }
            else if (!parsingKey && next == ';')
            {
                // At end of name/value pair, so insert values.
                String key = keyBuf.toString().trim();
                String value = valueBuf.toString().trim();
                props.setProperty(key, value);
                parsingKey = true;
                keyBuf = new StringBuffer();
                valueBuf = new StringBuffer();
            }
            else
            {
                valueBuf.append(next);
            }
        }

        // If there is a left over name/value pair, append.
        String key = keyBuf.toString().trim();
        String value = valueBuf.toString().trim();
        if (key.length() > 0 && value.length() > 0)
            props.setProperty(key, value);

        // Perform substitutions if desired, then load properties.
        if (doSubstitutions)
            substituteSystemValues(props);
        load(props);
    }

    /**
     * Load values from a JSON serialized string
     * 
     * @param json The JSON serialized string
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static TungstenProperties loadFromJSON(String json)
            throws JsonParseException, JsonMappingException, IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        TungstenProperties tungstenProp = mapper.readValue(json,
                TungstenProperties.class);

        return tungstenProp;
    }

    /**
     * Get an instances of Properties that is populated by the current values of
     * the instance on which it's called.
     * 
     * @return Properties
     */
    public Properties getProperties()
    {
        Properties props = new Properties();
        props.putAll(properties);

        return props;
    }

    /**
     * Load values from a Properties instance. Current values are obliterated.
     */
    public void load(Properties props)
    {
        HashMap<String, Object> map = new HashMap<String, Object>();
        Enumeration<?> keys = props.propertyNames();
        while (keys.hasMoreElements())
        {
            String key = (String) keys.nextElement();
            map.put(key, props.getProperty(key));
        }
        properties = map;
    }

    /**
     * Substitute system values up to a certain number of times. This permits
     * clients to reuse variables to get multiple substitutions without running
     * into problems with infinite loops.
     */
    public static int substituteSystemValues(Properties props, int iterations)
    {
        int substitutions = 0;
        // Substitute until we exhaust either iterations or substitutions.
        for (int i = 0; i < iterations; i++)
        {
            int count = substituteSystemValues(props);
            if (count == 0)
                break;
            else
                substitutions += count;
        }
        return substitutions;
    }

    /**
     * Scan Properties instance values replacing any expression of the form
     * ${name} where 'name' is a key in System.properties *or* a local property
     * in the same file, where local property substitutions take priority over
     * system property names. If there is no such value the expression is left
     * as is.
     */
    public static int substituteSystemValues(Properties props)
    {
        int substitutions = 0;

        // Make a copy of the properties object for local variable
        // substitutions.
        Properties originalProps = new Properties();
        originalProps.putAll(props);

        // Perform substitutions.
        Enumeration<Object> en = props.keys();
        while (en.hasMoreElements())
        {
            String key = (String) en.nextElement();
            String value = props.getProperty(key);
            if (value == null)
                continue;

            StringBuffer newValue = new StringBuffer();
            StringBuffer expression = null;
            StringBuffer name = null;

            // Execute a simple state machine to find and resolve
            // property name expressions.
            ParseState state = ParseState.NONE;
            for (int i = 0; i < value.length(); i++)
            {
                char c = value.charAt(i);
                switch (state)
                {
                    case NONE :
                        // Look for a $ indicating start of expression.
                        if (c == '$')
                        {
                            state = ParseState.DOLLAR;
                            expression = new StringBuffer();
                            expression.append(c);
                        }
                        else
                            newValue.append(c);
                        break;
                    case DOLLAR :
                        // Look for a left bracket.
                        expression.append(c);
                        if (c == '{')
                        {
                            state = ParseState.LBRACKET;
                        }
                        else
                        {
                            state = ParseState.NONE;
                            newValue.append(expression.toString());
                            expression = null;
                        }
                        break;
                    case LBRACKET :
                        // Look for the start of the properties name.
                        expression.append(c);
                        if (Character.isLetterOrDigit(c))
                        {
                            state = ParseState.NAME;
                            name = new StringBuffer();
                            name.append(c);
                        }
                        else
                        {
                            state = ParseState.NONE;
                            newValue.append(expression.toString());
                            expression = null;
                        }
                        break;
                    case NAME :
                        // Accumulate the properties name to right bracket.
                        expression.append(c);
                        if (c == '}')
                        {
                            // Try to translate first a local property then a
                            // system property.
                            String embeddedKey = name.toString();
                            if (embeddedKey.length() > 0)
                            {
                                String originalValue = originalProps
                                        .getProperty(embeddedKey);
                                String systemValue = System
                                        .getProperty(embeddedKey);
                                if (originalValue != null)
                                {
                                    expression = new StringBuffer(originalValue);
                                    substitutions++;
                                }
                                else if (systemValue != null)
                                {
                                    expression = new StringBuffer(systemValue);
                                    substitutions++;
                                }
                            }
                            name = null;
                            state = ParseState.NONE;
                            newValue.append(expression.toString());
                            expression = null;
                        }
                        else
                        {
                            name.append(c);
                        }
                        break;
                }
            }

            // If we still have an expression value left over, we need to apply
            // it to the new value.
            if (expression != null)
            {
                newValue.append(expression);
            }

            // Finally, set the new value.
            props.setProperty(key, newValue.toString());
        }

        // Return the total number of substitutions.
        return substitutions;
    }

    /**
     * Stores values in Java properties file format. This does not work for
     * embedded lists.
     */
    @SuppressWarnings("serial")
    public void store(OutputStream os) throws IOException
    {
        Properties props = null;
        if (sorted)
        {
            props = new Properties()
            {
                @Override
                public Set<Object> keySet()
                {
                    return Collections.unmodifiableSet(new TreeSet<Object>(
                            super.keySet()));
                }

                @Override
                public synchronized Enumeration<Object> keys()
                {
                    return new Enumeration<Object>()
                    {
                        private Iterator<Object> iterator;
                        {
                            iterator = keySet().iterator();
                        }

                        public boolean hasMoreElements()
                        {
                            return iterator.hasNext();
                        }

                        public Object nextElement()
                        {
                            return iterator.next();
                        }

                    };
                }
            };
        }
        else
        {
            props = new Properties();
        }
        for (String key : properties.keySet())
        {
            if (this.getString(key) != null)
                props.setProperty(key, this.getString(key));
        }
        props.store(os, "Tungsten properties");
    }

    /**
     * Applies the current properties to the given object, stopping and throwing
     * errors if a property has no matching setter in the given object. This is
     * equivalent to <code>applyProperties(o, false)</code>
     * 
     * @param o instance on which to set properties
     */
    public void applyProperties(Object o)
    {
        applyProperties(o, false);
    }

    /**
     * Applies the properties in the TungstenProperties object to corresponding
     * properties on a Java object by matching property names to setter methods
     * on the object. Here are the rules for matching.
     * <ol>
     * <li>The first letter of the property is capitalized, so foo_bar becomes
     * Foo_bar.</li>
     * <li>If an underscore ("_") occurs in the property name, it is omitted and
     * the following character, if any, is capitalized. Foo_bar becomes FooBar.</li>
     * <li>The prefix "set" is added to the result. FooBar becomes setFooBar.
     * </ol>
     * The setter method, if found, must have a single argument and must be
     * publicly accessible. Multiple setters that differ only by argument type
     * are not supported.
     * 
     * @param o Instance for which we are to set properties
     * @param ignoreIfMissing whether or not stop and throw an error if a
     *            property has no matching setting in the given object instance
     * @throws PropertyException Thrown if we cannot find a setter for a
     *             property or if invocation of the setter fails
     */
    public void applyProperties(Object o, boolean ignoreIfMissing)
    {
        // Find methods on this instance.
        Method[] methods = o.getClass().getMethods();

        // Allocated maps to hold data for Java bean classes (i.e., embedded
        // objects).
        Map<String, TungstenProperties> beanMaps = new HashMap<String, TungstenProperties>();

        // Extract all property names that have "." in them. These are
        // properties that apply to embedded Java Beans. The values need to go
        // into a map for each bean.
        for (String key : keyNames())
        {
            // If key has a period in it followed by a suffix, it is a bean
            // property.
            int period = key.indexOf('.');
            if (period == -1 || (period + 1) >= key.length())
                continue;

            // Determine the prefix and find the corresponding value map. Make
            // a new one if it is not there.
            String prefix = key.substring(0, period);
            TungstenProperties beanProps = beanMaps.get(prefix);
            if (beanProps == null)
            {
                beanProps = new TungstenProperties();
                beanMaps.put(prefix, beanProps);
            }

            // Add the property value to the bean properties.
            beanProps.put(key.substring(period + 1), getString(key));
        }

        // Try to find and invoke setter method corresponding to each property.
        for (String key : keyNames())
        {
            // Skip keys for embedded beans, i.e, those with dots. They will be
            // addressed later.
            int period = key.indexOf('.');
            if (period > 1 && (period + 1) < key.length())
                continue;

            // Construct setter name.
            StringBuffer setterNameBuffer = new StringBuffer();
            setterNameBuffer.append("set");
            char prev = '\0';
            for (int i = 0; i < key.length(); i++)
            {
                char c = key.charAt(i);
                if (i == 0)
                {
                    // Upper case first character.
                    setterNameBuffer.append(Character.toUpperCase(c));
                }
                else if (prev == '\0')
                {
                    // Append ordinary character unless it's an underscore.
                    if (c == '_')
                        prev = c;
                    else
                        setterNameBuffer.append(c);
                }
                else
                {
                    // Upper case character following an underscore.
                    setterNameBuffer.append(Character.toUpperCase(c));
                    prev = '\0';
                }
            }
            // Don't forget to add a trailing underscore.
            if (prev != '\0')
                setterNameBuffer.append(prev);

            String setterName = setterNameBuffer.toString();

            // Find a setter on the instance class if it exists.
            Method setter = null;
            for (Method m : methods)
            {
                if (!m.getName().equals(setterName))
                    continue;
                else if (m.getParameterTypes().length != 1)
                    continue;
                else
                {
                    setter = m;
                    break;
                }
            }
            if (setter == null)
            {
                if (ignoreIfMissing)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("Ignoring missing setter for property="
                                + key);
                    continue;
                }

                throw new PropertyException(
                        "Unable to find method corresponding to property: "
                                + " class=" + o.getClass().getName()
                                + " property=" + key + " expected setter="
                                + setterName);
            }

            // Construct the argument.
            String value = getString(key);

            if (value == null)
                continue;

            Object arg = null;
            // Next two lines generate Eclipse warnings.
            Class<?>[] argTypes = setter.getParameterTypes();
            Class<?> arg0Type = argTypes[0];
            // Handle primitive types
            if (arg0Type.isPrimitive())
            {
                try
                {
                    if (arg0Type == Integer.TYPE)
                        arg = new Integer(value);
                    else if (arg0Type == Long.TYPE)
                        arg = new Long(value);
                    else if (arg0Type == Boolean.TYPE)
                        arg = new Boolean(value);
                    else if (arg0Type == Character.TYPE)
                        arg = new Character(value.charAt(0));
                    else if (arg0Type == Float.TYPE)
                        arg = new Float(value);
                    else if (arg0Type == Double.TYPE)
                        arg = new Double(value);
                    else if (arg0Type == Byte.TYPE)
                        arg = new Byte(value);
                    else if (arg0Type == Short.TYPE)
                        arg = new Short(value);
                }
                catch (Exception e)
                {
                    throw new PropertyException(
                            "Unable to translate property value: key=" + key
                                    + " value = " + value, e);
                }
            }
            // Special storage methods:
            else if (arg0Type == Date.class)
            {
                try
                {
                    // Date type is stored as a long provided by
                    // java.util.Date#getTime()
                    arg = new Date(new Long(value));
                }
                catch (Exception e)
                {
                    throw new PropertyException(
                            "Unable to translate property value: key=" + key
                                    + " value = " + value, e);
                }
            }
            else if (arg0Type == List.class)
            {
                arg = Arrays.asList(value.split(","));
            }
            else
            {
                // For other types, try three methods:
                // 1. Try to treat it as a bean.
                arg = constructBean(value, beanMaps.get(key), ignoreIfMissing);

                // 2. Try to call Constructor(String)
                if (arg == null)
                    arg = constructFromString(arg0Type, value);

                // 3. Try to call <Type>.valueOf(String)
                if (arg == null)
                    arg = constructUsingValueOf(arg0Type, value);

                // At this point we are done and need to give up.
                if (arg == null)
                {
                    if (ignoreIfMissing)
                    {
                        continue;
                    }
                    logger.warn("Could not instantiate non-bean arg of type "
                            + arg0Type
                            + ". No Constructor(String) nor valueOf(String) found in this class");
                    throw new PropertyException(
                            "Unsupported property type: key=" + key + " type="
                                    + arg0Type + " value=" + value);
                }
            }

            // Now set the value.
            try
            {
                setter.invoke(o, new Object[]{arg});
                if (logger.isDebugEnabled() == true)
                {
                    logger.debug("Set attribute in object=<"
                            + o.getClass().getSimpleName() + "> from key <"
                            + key + ">");
                }
            }
            catch (Exception e)
            {
                throw new PropertyException("Unable to set property: key="
                        + key + " value = " + value, e);
            }
        }
    }

    // Try to instantiate and configure a bean.
    private Object constructBean(String className,
            TungstenProperties beanProps, boolean ignoreIfMissing)
    {
        // Return null if we don't support beans.
        if (!this.isBeanSupportEnabled())
            return null;

        // Load the class.
        Class<?> type = null;
        try
        {
            type = Class.forName(className);
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Unable to load bean class: name=" + className, e);
            }
            return null;
        }

        // If this does not look like a bean, just return.
        if (!isBean(type))
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("Class does not meet bean qualifications: name="
                        + className);
            }
            return null;
        }

        // Try to instantiate.
        Object arg = null;
        try
        {
            arg = type.newInstance();
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(
                        "Unable to instantiate class using default constructor: name="
                                + type.getName(), e);
            }
            return null;
        }

        // Assign values.
        if (beanProps != null)
            beanProps.applyProperties(arg, ignoreIfMissing);

        return arg;
    }

    // Try to instantiate a type using the String constructor.
    private Object constructFromString(Class<?> type, String value)
    {
        try
        {
            Object arg = type.getConstructor(String.class).newInstance(value);
            if (logger.isTraceEnabled())
                logger.trace("String constructor for arg type " + type
                        + " found. arg value is " + value);
            return arg;

        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(
                        "Unable to instantiate class using string constructor: name="
                                + type.getName() + " value=" + value, e);
            }
            return null;
        }
    }

    // Try to instantiate a type using valueOf() method.
    private Object constructUsingValueOf(Class<?> type, String value)
    {
        try
        {
            Object arg = type.getMethod("valueOf", new Class[]{String.class})
                    .invoke(type, value);
            if (logger.isTraceEnabled())
                logger.trace("Method valueOf(String) for arg type " + type
                        + " found. arg value is " + arg);
            return arg;
        }
        catch (Exception e)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("No valueOf(String) method found for arg type "
                        + type, e);
            }
            return null;
        }
    }

    /**
     * Create a set of initialized properties from the set of fields in a
     * specific object instance.
     * 
     * @param o
     * @param ignoreIfUnsupported
     */
    @SuppressWarnings("unchecked")
    public void extractProperties(Object o, boolean ignoreIfUnsupported)
    {
        if (logger.isDebugEnabled())
            logger.debug("Extracting properties from object="
                    + o.getClass().getName());
        Field[] fields = o.getClass().getDeclaredFields();

        for (Field field : fields)
        {
            field.setAccessible(true);
            if (logger.isDebugEnabled())
                logger.debug("Extracting field=" + field.getName());
            try
            {
                if (field.getType() == Integer.TYPE)
                {
                    this.setInt(field.getName(), field.getInt(o));
                }
                else if (field.getType() == Long.TYPE)
                {
                    this.setInt(field.getName(), (int) field.getLong(o));
                }
                else if (field.getType() == Boolean.TYPE)
                {
                    this.setBoolean(field.getName(), field.getBoolean(o));
                }
                else if (field.getType() == String.class)
                {
                    this.setString(field.getName(), (String) field.get(o));
                }
                else if (field.getType() == Float.TYPE)
                {
                    this.setFloat(field.getName(), (Float) field.get(o));
                }
                else if (field.getType() == Double.TYPE)
                {
                    this.setDouble(field.getName(), (Double) field.get(o));
                }
                else if (field.getType() == Date.class)
                {
                    this.setDate(field.getName(), (Date) field.get(o));
                }
                else if (field.getType() == List.class)
                {
                    this.setStringList(field.getName(),
                            (List<String>) field.get(o));
                }
                else
                {
                    // If we have a type that follows bean conventions, try to
                    // extract its properties.
                    Object bean = field.get(o);
                    if (bean == null)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Skipping property with null value, prop="
                                    + field.getName());
                        }
                    }
                    else if (isBean(bean.getClass()))
                    {
                        // Extract bean properties.
                        TungstenProperties beanProps = new TungstenProperties();
                        beanProps.extractProperties(bean, ignoreIfUnsupported);

                        // Write the bean class name.
                        String beanKey = field.getName();
                        setString(beanKey, bean.getClass().getName());

                        // Write the properties of the bean adding the bean key
                        // as a prefix.
                        this.putAllWithPrefix(beanProps, beanKey + ".");
                    }
                    else if (ignoreIfUnsupported)
                    {
                        if (logger.isDebugEnabled())
                        {
                            logger.debug("Skipping property with unsupported type, prop="
                                    + field.getName());
                        }
                        continue;
                    }
                    else
                    {
                        throw new PropertyException(
                                "Unsupported property type:" + field.getType());
                    }
                }
            }
            catch (IllegalAccessException i)
            {
                logger.error("Exception while trying to extract values from field="
                        + field.getName()
                        + " of class="
                        + o.getClass().getName());
            }
        }
    }

    /**
     * Trims all property values to remove leading and trailing whitespace.
     */
    public void trim()
    {
        for (String key : keyNames())
        {
            String value = (String) properties.get(key);
            if (value != null)
                properties.put(key, value.trim());
        }
    }

    /**
     * Removes the property indicated by the key, if it exists, and returns the
     * value.
     */
    public String remove(String key)
    {
        String value = get(key);
        properties.remove(key);
        return value;
    }

    /**
     * Clears all property values so that the underlying map is empty.
     */
    public void clear()
    {
        properties.clear();
    }

    /**
     * Returns true if properties map is empty.
     */
    @JsonIgnore
    public boolean isEmpty()
    {
        return properties.isEmpty();
    }

    /**
     * Returns keys of all properties currently stored in this instance.
     */
    public Set<String> keyNames()
    {
        return properties.keySet();
    }

    /**
     * Returns keys of all properties where the key name matches the provided
     * prefix.
     */
    public Set<String> keyNames(String prefix)
    {
        Set<String> keys = keyNames();
        HashSet<String> subset = new HashSet<String>();
        for (String key : keys)
        {
            if (key != null && key.startsWith(prefix))
                subset.add(key);
        }
        return subset;
    }

    /**
     * Returns true if the indicated property key exists. This method returns
     * true even if the property is set to null.
     */
    public boolean containsKey(String key)
    {
        return (properties.containsKey(key));
    }

    /**
     * Returns the number of properties in this instance.
     */
    public int size()
    {
        return properties.size();
    }

    /**
     * Merges the properties provided as an argument with these properties,
     * overriding current values wherever there are overlaps.
     */
    public void putAll(TungstenProperties props)
    {
        properties.putAll(props.map());
    }

    /**
     * Prefixes all the given properties with the given string and merges this
     * new set with the actual ones, overriding current values with same keys if
     * any
     */
    public void putAllWithPrefix(TungstenProperties props, String prefix)
    {
        TungstenProperties newProps = new TungstenProperties();
        if (props.isEmpty())
        {
            newProps.setObject(prefix, props);
        }
        else
        {
            for (String key : props.keyNames())
            {
                newProps.setObject(prefix + key, props.getObject(key));
            }
        }
        putAll(newProps);
    }

    /**
     * Sets the value as a string.
     */
    public void setString(String key, String value)
    {
        properties.put(key, value);
    }

    /**
     * Utility method to help with compatibility with Properties
     * 
     * @param key
     * @param value
     */
    public void setProperty(String key, String value)
    {
        setString(key, value);
    }

    public void put(Object key, Object value)
    {
        setObject((String) key, value);
    }

    /**
     * Utility method to help with compatibility with Properties
     * 
     * @param key
     * @param value
     */
    public void put(String key, String value)
    {
        setString(key, value);
    }

    /**
     * Sets the property value from an object using its toString() method.
     */
    public void setObject(String key, Object value)
    {
        if (value == null)
            properties.put(key, null);
        else
            properties.put(key, value);
    }

    public void setInt(String key, int value)
    {
        properties.put(key, Integer.toString(value));
    }

    public void setLong(String key, long value)
    {
        properties.put(key, Long.toString(value));
    }

    public void setFloat(String key, float value)
    {
        properties.put(key, Float.toString(value));
    }

    public void setDouble(String key, double value)
    {
        properties.put(key, Double.toString(value));
    }

    public void setBoolean(String key, boolean value)
    {
        properties.put(key, Boolean.toString(value));
    }

    public void setFile(String key, File value)
    {
        properties.put(key, value.toString());
    }

    /**
     * Date type is stored as a long representing the number of milliseconds
     * since January 1, 1970, 00:00:00 GMT as retrieved by
     * {@link Date#getTime()}
     * 
     * @param key unique identifier for this property
     * @param value the date to store
     */
    public void setDate(String key, Date value)
    {
        if (value != null)
            setLong(key, value.getTime());
    }

    /**
     * Sets a value from a String list. This results in a series of
     * comma-separated values that can be read with
     * {@link #getStringList(String)}.
     */
    public void setStringList(String key, List<String> list)
    {
        StringBuffer sb = new StringBuffer();
        if (list == null)
        {
            setString(key, null);
        }
        else
        {
            for (String value : list)
            {
                if (sb.length() > 0)
                    sb.append(",");
                sb.append(value);
            }
            setString(key, sb.toString());
        }
    }

    /**
     * Stores a time interval provided in milliseconds, which is the base
     * representation.
     */
    public void setInterval(String key, Long value)
    {
        setLong(key, value);
    }

    /**
     * Stores a time interval provided as an object by converting to an interval
     * instance.
     */
    public void setInterval(String key, Interval value)
    {
        setLong(key, value.longValue());
    }

    /**
     * Stores a TungstenProperties as a property
     * 
     * @param key the key to identify the property
     * @param tungstenProperties the TungstenProperties to store
     */
    public void setTungstenProperties(String key,
            TungstenProperties tungstenProperties)
    {
        properties.put(key, tungstenProperties);
    }

    /**
     * Stores a Map which keys are strings and values are TungstenProperties.
     * This function is meant to be used to store a data source map
     * <p>
     * Each TungstenProperties entry in the given map will be stored in this
     * object with its keys prefixed by the corresponding map key + its own key,
     * separated by MAP_KEY_SEPARATOR
     * <p>
     * Note that none of the TungstenProperties in the given map can be null
     * 
     * @param map the map to store
     */
    public void setDataSourceMap(Map<String, TungstenProperties> map)
    {
        for (String key : map.keySet())
        {
            putAllWithPrefix(map.get(key), key + MAP_KEY_SEPARATOR);
        }
    }

    /**
     * Stores a Map which keys are strings and values are Maps of
     * String/TungstenProperties. This function is meant to be used to store a
     * cluster map
     * <p>
     * Each entry in the given map will be stored in this object with its keys
     * prefixed by the corresponding map key + MAP_KEY_SEPARATOR + its own key +
     * MAP_KEY_SEPARATOR + its Tungsten properties key.
     * <p>
     * Note that none of the TungstenProperties in the given map can be null
     * 
     * @param map the map to store
     */
    public void setClusterMap(Map<String, Map<String, TungstenProperties>> map)
    {
        for (String key : map.keySet())
        {
            Map<String, TungstenProperties> val = map.get(key);
            for (String valKey : val.keySet())
            {
                putAllWithPrefix(val.get(valKey), key + MAP_KEY_SEPARATOR
                        + valKey + MAP_KEY_SEPARATOR);
            }
        }
    }

    /**
     * Given a key, gets the object
     * 
     * @param key
     * @param defaultValue
     * @param required
     * @return Object
     */
    public Object getObject(String key, Object defaultValue, boolean required)
    {
        // return (String) getObject(key, defaultValue, required);
        Object value = properties.get(key);
        if (value != null)
            return value;
        else if (defaultValue != null)
            return defaultValue;

        if (required)
            throw new PropertyException(
                    "No value found for required property: " + key);
        else
            return null;
    }

    /**
     * Returns the value as a String with an optional default value and checking
     * to ensure value is present if required
     * 
     * @param key The name of the property
     * @param defaultValue An optional default value
     * @param required If true, a value or default must be present
     * @return The corresponding value or null if not found and no default
     *         exists
     * @throws PropertyException if the value is required but does not exist
     */
    public String getString(String key, String defaultValue, boolean required)
    {
        Object o = getObject(key, defaultValue, required);
        if (o != null)
            return o.toString();
        return null;
    }

    /**
     * Returns the value as a String or null if not found
     */
    public String getString(String key)
    {
        return getString(key, null, false);
    }

    public String getProperty(String key, String defaultValue)
    {
        return getString(key, defaultValue, false);
    }

    public String getProperty(String key)
    {
        return getString(key);
    }

    public String get(String key)
    {
        return getString(key);
    }

    public Object getObject(String key)
    {
        return getObject(key, null, false);
    }

    public int getInt(String key)
    {
        return getInt(key, null, false);
    }

    public int getInt(String key, String defaultValue, boolean required)
    {
        return Integer.parseInt(getString(key, defaultValue, required));
    }

    public long getLong(String key)
    {
        return getLong(key, null, false);
    }

    public long getLong(String key, String defaultValue, boolean required)
    {
        return Long.parseLong(getString(key, defaultValue, required));
    }

    public float getFloat(String key)
    {
        return getFloat(key, null, false);
    }

    public float getFloat(String key, String defaultValue, boolean required)
    {
        return Float.parseFloat(getString(key, defaultValue, required));
    }

    public double getDouble(String key)
    {
        return getDouble(key, null, false);
    }

    public double getDouble(String key, String defaultValue, boolean required)
    {
        return Double.parseDouble(getString(key, defaultValue, required));
    }

    public boolean getBoolean(String key)
    {
        return getBoolean(key, null, false);
    }

    public boolean getBoolean(String key, String defaultValue, boolean required)
    {
        return Boolean.parseBoolean(getString(key, defaultValue, required));
    }

    public File getFile(String key)
    {
        return new File(getString(key));
    }

    public File getFile(String key, String defaultValue, boolean required)
    {
        return new File(getString(key, defaultValue, required));
    }

    public Date getDate(String key, String defaultValue, boolean required)
    {
        return new Date(Long.parseLong(getString(key, defaultValue, required)));
    }

    public Date getDate(String key)
    {
        return getDate(key, null, false);
    }

    /**
     * Returns a list of strings from a value containing a list of string values
     * separated by commas or whitespace characters. Here are some examples:
     * <ul>
     * <li>"a,b,c" returns "a", "b", "c"</li>
     * <li>" a b c, " returns "a", "b", "c"</li>
     * <li>"," returns an empty list
     * <li>"I think so but, it's not!" returns "I", "think", "so", "but",
     * "it's", and "not!"
     * </ul>
     */
    public List<String> getStringList(String key)
    {
        List<String> list = new ArrayList<String>();
        String listValues = getString(key);
        if (listValues == null)
            return list;
        else if (listValues instanceof String)
        {
            StringTokenizer st = new StringTokenizer(listValues, ", \t\n\r\f");
            while (st.hasMoreTokens())
                list.add(st.nextToken());
            return list;
        }
        else
        {
            throw new PropertyException(
                    "Invalid type for comma-separated list: "
                            + listValues.getClass().getName());
        }
    }

    /**
     * Returns an interval or the default value.
     */
    public Interval getInterval(String key, String defaultValue,
            boolean required)
    {
        return new Interval(getString(key, defaultValue, required));
    }

    /**
     * Returns an interval value.
     */
    public Interval getInterval(String key)
    {
        return getInterval(key, null, false);
    }

    /**
     * Returns a TungstenProperties value.
     * 
     * @param key identifying the property
     * @return TungstenProperties
     */
    @SuppressWarnings("unchecked")
    public TungstenProperties getTungstenProperties(String key)
    {
        TungstenProperties tungstenProp = null;

        Object value = this.getObject(key);

        if (value instanceof TungstenProperties)
            tungstenProp = (TungstenProperties) value;
        else if (value instanceof LinkedHashMap<?, ?>)
            tungstenProp = new TungstenProperties((Map<String, String>) value);

        return tungstenProp;
    }

    /**
     * Retrieves a data source map as stored by {@link #setDataSourceMap(Map)}
     * 
     * @return a String/TungstenProperties map
     */
    @JsonIgnore
    public Map<String, TungstenProperties> getDataSourceMap()
    {
        Map<String, TungstenProperties> result = new HashMap<String, TungstenProperties>();
        Set<String> keys = keyNames();
        while (!keys.isEmpty())
        {
            String key = keys.iterator().next();
            String realKey = key.substring(0, key.indexOf(MAP_KEY_SEPARATOR));

            result.put(realKey, subset(realKey + MAP_KEY_SEPARATOR, true, true));
            keys = keyNames();
        }
        return result;
    }

    /**
     * Retrieves a cluster map as stored by {@link #setClusterMap(Map)}
     * 
     * @return a String/(String/TungstenProperties) map
     */
    @JsonIgnore
    public Map<String, Map<String, TungstenProperties>> getClusterMap()
    {
        Map<String, Map<String, TungstenProperties>> fullResult = new HashMap<String, Map<String, TungstenProperties>>();
        Set<String> keys = keyNames();
        while (!keys.isEmpty())
        {
            String key = keys.iterator().next();
            int mapKeyIdx = key.indexOf(MAP_KEY_SEPARATOR);
            if (mapKeyIdx <= 0)
            {
                // not a cluster map entry, ignore it
                keys.remove(key);
                continue;
            }
            String serviceKey = key.substring(0, mapKeyIdx);
            Map<String, TungstenProperties> result = fullResult.get(serviceKey);
            if (result == null)
            {
                result = new HashMap<String, TungstenProperties>();
            }
            String valKey = key.substring(key.indexOf(MAP_KEY_SEPARATOR)
                    + MAP_KEY_SEPARATOR.length(),
                    key.lastIndexOf(MAP_KEY_SEPARATOR));
            result.put(
                    valKey,
                    subset(serviceKey + MAP_KEY_SEPARATOR + valKey
                            + MAP_KEY_SEPARATOR, true, true));
            fullResult.put(serviceKey, result);
            keys = keyNames();
        }
        return fullResult;
    }

    /**
     * Returns a shallow copy of the underlying map as a generic Map.
     */
    public Map<String, String> map()
    {
        return hashMap();
    }

    /**
     * Returns a shallow copy of the underlying map as a HashMap.
     */
    public HashMap<String, String> hashMap()
    {
        HashMap<String, String> retMap = new HashMap<String, String>();
        for (String key : properties.keySet())
        {
            Object value = properties.get(key);
            if (value != null)
            {
                retMap.put(key, value.toString());
            }
            else
            {
                retMap.put(key, null);
            }
        }

        return retMap;
    }

    /**
     * Returns values as a string of name/value pairs that can be loaded using
     * {@link #load(String, boolean)}.
     */
    public String toNameValuePairs()
    {
        StringBuffer pairs = new StringBuffer();
        for (String key : properties.keySet())
        {
            if (pairs.length() > 0)
                pairs.append(';');
            pairs.append(key).append("=").append(properties.get(key));
        }
        return pairs.toString();
    }

    /**
     * Returns a TungstenProperties instance consisting of the property names
     * that match the given prefix
     * 
     * @param prefix Return only those properties that match the prefix
     * @param removePrefix If true remove the prefix from each property name
     */
    public TungstenProperties subset(String prefix, boolean removePrefix)
    {
        return subset(prefix, removePrefix, false);
    }

    /**
     * Returns a TungstenProperties instance consisting of the property names
     * that match the given prefix
     * 
     * @param prefix Return only those properties that match the prefix
     * @param removePrefix If true remove the prefix from each property name
     * @param removeProps If true remove the matching key/value pairs from these
     *            properties
     */
    public TungstenProperties subset(String prefix, boolean removePrefix,
            boolean removeProps)
    {
        TungstenProperties tp = new TungstenProperties();
        Set<String> prefixKeys = keyNames(prefix);
        int nameIndex = 0;
        if (removePrefix)
            nameIndex = prefix.length();
        for (String key : prefixKeys)
        {
            String newKey = key.substring(nameIndex);
            if (newKey.length() > 0)
                tp.setObject(newKey, getObject(key));
            if (removeProps)
                remove(key);
        }
        return tp;
    }

    /**
     * Returns true if the argument contains exactly the same property values as
     * this properties instance.
     */
    public boolean equals(Object o)
    {
        if (!(o instanceof TungstenProperties))
            return false;

        Map<String, Object> tp2 = ((TungstenProperties) o).properties;
        return properties.equals(tp2);
    }

    /**
     * Returns toString of underlying properties rather than wrapper.
     */
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        TreeMap<String, Object> orderedProps = new TreeMap<String, Object>();

        orderedProps.putAll(properties);

        builder.append("\n{\n");

        int propCount = 0;
        for (String key : orderedProps.keySet())
        {

            Object value = orderedProps.get(key);

            // Skip processing null values...
            if (value == null)
            {
                continue;
            }

            if (++propCount > 1)
                builder.append("\n");

            builder.append("  ").append(key).append("=");

            if (value instanceof String)
            {
                // Strings must properly escape control characters.
                String valueAsString = (String) value;
                for (int i = 0; i < valueAsString.length(); i++)
                {
                    char c = valueAsString.charAt(i);
                    if (Character.isISOControl(c))
                    {
                        // Print Unicode escape sequence.
                        int cAsInt = c;
                        String escapedValue = String.format("\\u%04x", cAsInt);
                        builder.append(escapedValue);
                    }
                    else
                    {
                        // Otherwise just print the character representation.
                        builder.append(c);
                    }
                }
            }
            else
            {
                builder.append(value.toString());
            }

        }
        builder.append("\n}");

        return builder.toString();
    }

    public String toJSON() throws JsonGenerationException,
            JsonMappingException, IOException
    {
        return this.toJSON(false);
    }

    /**
     * Serialize the TungstenProperties into a JSON String
     * 
     * @param prettyPrint Set to true to have the JSON output formatted for
     *            easier read
     * @return String representing JSON serialization of the TungstenProperties
     * @throws JsonGenerationException
     * @throws JsonMappingException
     * @throws IOException
     */
    public String toJSON(boolean prettyPrint) throws JsonGenerationException,
            JsonMappingException, IOException
    {
        String json = null;
        ObjectMapper mapper = new ObjectMapper(); // Setup Jackson
        mapper.configure(Feature.INDENT_OUTPUT, true);
        mapper.configure(Feature.SORT_PROPERTIES_ALPHABETICALLY, true);

        ObjectWriter writer = mapper.writer();
        if (prettyPrint)
            writer = writer.withDefaultPrettyPrinter();

        json = writer.writeValueAsString(this);

        return json;
    }

    public static String formatProperties(String name,
            TungstenProperties props, String header)
    {
        String indent = "  ";
        StringBuilder builder = new StringBuilder();
        builder.append(header);

        builder.append(String.format("name = %s\n", name)).append(header);
        builder.append("{\n");
        Map<String, String> propMap = props.hashMap();
        for (String key : propMap.keySet())
        {
            builder.append(String.format("%s%s = %s\n", indent, key,
                    propMap.get(key)));
        }
        builder.append(String.format("}"));
        return builder.toString();
    }

    /**
     * Receives properties from given stream.<br>
     * This function uses a in-house protocol consisting in having, for each
     * key/value pair, 1 line for key, 1 line for the class name and 1 for
     * value. The end of the transmission is identified by a predefined key name
     * {@value #ENDOFPROPERTIES_TAG}
     * 
     * @see #send(PrintWriter)
     * @param in a ready-to-be-read buffered reader from which to get properties
     * @return a new set of properties containing data read on the stream
     * @throws IOException upon error while reading on the given input stream,
     *             or if no data can be read at all
     * @throws ClassNotFoundException
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws IllegalArgumentException
     */
    public static TungstenProperties createFromStream(BufferedReader in)
            throws IOException
    {
        String key = in.readLine();
        String valueClass = null;
        String valueLine = null;
        TungstenProperties tp = new TungstenProperties();
        while (key != null && !key.equals(ENDOFPROPERTIES_TAG))
        {
            valueClass = in.readLine();
            if (valueClass == null)
            {
                throw new IOException(
                        "Cannot create properties from stream: "
                                + "reached end of stream before end of properties tag\n"
                                + "Properties received till now:\n" + tp + "\n"
                                + "last property key=" + key + " value class="
                                + valueClass + " value=" + valueLine);
            }
            else if ("null".equals(valueClass))
            {
                tp.put(key, null);
            }
            else
            {
                Object value = null;
                if (TungstenProperties.class.getName().equals(valueClass))
                {
                    value = createFromStream(in);
                }
                else
                {
                    valueLine = in.readLine();
                    if (valueLine == null)
                        throw new IOException(
                                "Cannot create properties from stream: "
                                        + "reached end of stream before end of properties tag");
                    while (!valueLine.endsWith(ENDOFLINE_TAG))
                    {
                        String nextLine = in.readLine();
                        if (nextLine == null)
                            throw new IOException(
                                    "Cannot create properties from stream: "
                                            + "reached end of stream before end of properties tag");
                        valueLine = valueLine + "\n" + nextLine;
                    }
                    valueLine = valueLine.substring(0, valueLine.length()
                            - ENDOFLINE_TAG.length());
                    try
                    {
                        Constructor<?> ctor = Class.forName(valueClass)
                                .getConstructor(String.class);
                        value = ctor.newInstance(valueLine);
                    }
                    catch (Exception e)
                    {
                        String message = "Could not instanciate property class "
                                + valueClass + " with value " + value;
                        if (logger.isDebugEnabled())
                        {
                            logger.debug(message, e);
                        }
                        IOException toThrow = new IOException(message);
                        toThrow.setStackTrace(e.getStackTrace());
                        throw toThrow;
                    }
                }
                tp.put(key, value);
            }
            key = in.readLine();
        }
        // Data consistency check: the last key received must be the end of
        // properties tag, otherwise an error has occurred and we must throw an
        // exception
        if (!ENDOFPROPERTIES_TAG.equals(key))
            throw new IOException("Cannot create properties from stream: "
                    + "reached end of stream before end of properties tag");
        return tp;
    }

    /**
     * Sends this object's set of properties on the given stream. <br>
     * This function uses a in-house protocol consisting in having, for each
     * key/value pair, 1 line for key an 1 for value. The end of the
     * transmission is identified by a predefined key name
     * {@value #ENDOFPROPERTIES_TAG}
     * 
     * @see #createFromStream(BufferedReader)
     * @param out a prepared PrintWriter output stream on which to send
     *            properties
     */
    public void send(PrintWriter out)
    {
        for (String key : keyNames())
        {
            out.println(key);
            Object value = getObject(key);
            if (value == null)
                out.println("null");
            else
            {
                out.println(value.getClass().getName());
                if (value instanceof TungstenProperties)
                {
                    ((TungstenProperties) value).send(out);
                }
                else
                {
                    out.println(value.toString() + ENDOFLINE_TAG);
                }
            }
        }
        out.println(ENDOFPROPERTIES_TAG);
        out.flush();
    }

    /**
     * Returns true if the class in question is supports JavaBean conventions by
     * having a default constructor and setters/getters for properties.
     */
    public boolean isBean(Class<?> clazz)
    {
        // Return false if we don't support beans.
        if (!this.isBeanSupportEnabled())
            return false;

        // Check for a default constructor.
        boolean hasDefaultConstructor = false;
        for (Constructor<?> constructor : clazz.getConstructors())
        {
            if (constructor.getParameterTypes().length == 0)
            {
                hasDefaultConstructor = true;
                break;
            }
        }
        if (!hasDefaultConstructor)
            return false;

        // Check that we have at least one setter.
        Map<String, Method> setters = getMethods(clazz, "set");
        if (setters.size() == 0)
            return false;

        // Check that each setter has a matching getter.
        Map<String, Method> getters = getMethods(clazz, "get");
        Map<String, Method> booleanGetters = getMethods(clazz, "is");
        for (String name : setters.keySet())
        {
            String getterName = "get" + name.substring(3);
            String booleanGetterName = "is" + name.substring(3);

            if (getters.containsKey(getterName)
                    || booleanGetters.containsKey(booleanGetterName))
                continue;
            else
                return false;
        }

        // Looks like a bean.
        return true;
    }

    // Returns all methods that start with a particular prefix and have
    // at least one additional character beyond to represent the property
    // name.
    private Map<String, Method> getMethods(Class<?> clazz, String prefix)
    {
        Map<String, Method> methodMap = new HashMap<String, Method>();
        for (Method method : clazz.getMethods())
        {
            String name = method.getName();
            if (name.startsWith(prefix) && name.length() > prefix.length())
                methodMap.put(method.getName(), method);
        }
        return methodMap;
    }

    /**
     * Generate a comma-delimited string of list items.
     */
    static public String listToString(List<String> list)
    {
        StringBuilder builder = new StringBuilder();
        int itemCount = 0;
        for (String value : list)
        {
            itemCount++;
            if (itemCount > 1)
            {
                builder.append(",");
            }
            builder.append(value);
        }

        return builder.toString();
    }

    /**
     * Loads values from Java properties file format with variable
     * substitutions.
     */
    public void add(InputStream is) throws IOException
    {
        add(is, true);
    }

    /**
     * Adds values from Java properties file format. Current values are kept
     * except if it exists in the stream, in which case it is overwritten.
     * 
     * @param is InputStream containing properties.
     * @param doSubstitutions If true perform variable substitutions
     */
    public void add(InputStream is, boolean doSubstitutions) throws IOException
    {
        // Load the properties file.
        Properties props = new Properties();
        props.load(is);
        if (doSubstitutions)
            substituteSystemValues(props);
        add(props);
    }

    /**
     * Load values from a Properties instance. Current values are replaced only
     * if they are in the source map.
     */
    public void add(Properties props)
    {
        Enumeration<?> keys = props.propertyNames();
        while (keys.hasMoreElements())
        {
            String key = (String) keys.nextElement();
            String value = props.getProperty(key).toString();
            if (properties.get(key) != null)
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(String.format("Replacing %s=%s with %s=%s",
                            key, properties.get(key), key, value));
                }
            }

            properties.put(key, value);
        }
    }

}