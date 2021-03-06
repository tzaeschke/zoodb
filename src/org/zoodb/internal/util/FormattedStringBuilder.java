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
package org.zoodb.internal.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * This is a convenience class with various improvements to 
 * <tt>StringBuilder</tt>.
 * <p>
 * It provides for example <tt>append()</tt> and <tt>appendln()</tt>, both with 
 * varargs. The latter is using the platform specific line break characters 
 * from <tt>System.getProperty("line.separator")</tt>.
 * <p>
 * This class also provides a convenience method <tt>{@link #fill(int, char)} 
 * </tt> to append a number of characters to an existing buffer.
 * <p>
 * Most methods also return the updated instance in order to allow chaining
 * of commands, e.g. <tt>buf.append("123.5435").pad(10);</tt>
 * <p>
 * Special version of <tt>append()</tt> and <tt>appendln()</tt> have been added,
 * which take <tt>Throwable</tt>s as argument, allowing easy printing of 
 * exceptions.
 *
 * @author Tilmann Zaeschke
 */
public class FormattedStringBuilder {

    /** New Line. */
    public final static String NL = System.getProperty("line.separator");
    
    private StringBuilder _delegate;
    
    /**
     * Creates a new <tt>FormattedStringBuilder</tt>.
     */
    public FormattedStringBuilder() {
        _delegate = new StringBuilder();
    }

    /**
     * Creates a new <tt>FormattedStringBuilder</tt> with the given initial content.
     * @param initial The initial string value
     */
    public FormattedStringBuilder(String initial) {
        _delegate = new StringBuilder(initial);
    }

    /**
     * Appends the specified string(s) to this character sequence.
     * @param strings The Strings to append
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder append(String ... strings) {
        for(String s: strings) {
            _delegate.append(s);
        }
        return this;
    }

    /**
     * Appends the specified string(s) to this character sequence and 
     * after all strings appended add a new line.
     * @param strings The Strings to append
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder appendln(String ... strings) {
        append(strings);
        _delegate.append(NL);
        return this;
    }
    
    /**
     * Appends the string representation of the char argument to this sequence.
     * @param c The character to append
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder append(char c) {
        _delegate.append(c);
        return this;
    }

    /**
     * Appends the string representation of the int argument to this sequence.
     * @param i The integer to append
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder append(int i) {
        _delegate.append(i);
        return this;
    }
    
    /**
     * Appends the stack trace of the Throwable argument to this sequence.
     * @param t Exception to print.
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder append(Throwable t) { 
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream p = new PrintStream(os);
        t.printStackTrace(p);
        p.close();
        return append(os.toString());
    }

    /**
     * Appends the stack trace of the Throwable argument to this sequence 
     * and then a new line.
     * @param t Exception to print.
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder appendln(Throwable t) {
        return append(t).appendln();
    }

    /**
     * Appends to the line the white spaces and then the string <tt>s</tt>
     * so that the end of the string <tt>s</tt> is at the position 
     * <tt>alignPosition</tt>. If the string is longer than the free space 
     * in the buffer to the align position then an IllegalArgumentException 
     * is thrown.
     * 
     * @param s String to append.
     * @param alignPosition index of the last character of the string 
     * relative to the current line in the buffer.
     * @return The updated instance of FormattedStringBuilder.
     * @throws IllegalArgumentException if not enough space is allocated for 
     * the string.
     */
    public FormattedStringBuilder appendRightAligned(String s, int alignPosition) {
        int lineStart = _delegate.lastIndexOf(NL);
        int currentLength = _delegate.length();
        int rightPosition = alignPosition;
        if(lineStart != -1) {
            lineStart += NL.length();
            rightPosition = lineStart + alignPosition;
        }
        int fillPosition = rightPosition - s.length();
        if(fillPosition<currentLength) {
            throw new IllegalArgumentException("String \"" + s + "\" to right "
                + "align is longer than the unfilled buffer space ["
                + (rightPosition - currentLength) + "]");
        } else {
            fillBuffer(fillPosition, ' ');
        }
        return append(s);
    }

    /**
     * Attempts to append <tt>c</tt> until the buffer gets the length <tt>
     * newLength</tt>. If the buffer is already as long or longer than <tt>
     * newLength</tt>, then nothing is appended.
     * @param newLength Minimal new length of the returned buffer.
     * @param c Character to append.
     * @return The updated instance of FormattedStringBuilder.
     */
    private FormattedStringBuilder fillBuffer(int newLength, char c) {
        for (int i = _delegate.length(); i < newLength; i++ ) {
            _delegate.append(c);
        }
        return this;
    }
    
    /**
     * Attempts to append <tt>c</tt> until the current line gets the length <tt>
     * newLength</tt>. If the line is already as long or longer than <tt>
     * newLength</tt>, then nothing is appended.
     * @param newLength Minimal new length of the last line.
     * @param c Character to append.
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder fill(int newLength, char c) {
        int lineStart = _delegate.lastIndexOf(NL);
        if (lineStart == -1) {
            return fillBuffer(newLength, c);
        }
        lineStart += NL.length();
        return fillBuffer(lineStart + newLength, c);
    }
    
    /**
     * Attempts to append white spaces until the current line gets the length <tt>
     * newLength</tt>. If the line is already as long or longer than <tt>
     * newLength</tt>, then nothing is appended. 
     *
     * @param newLength Minimal new length of the last line.
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder fill(int newLength) {
        return fill(newLength, ' ');
    }
    
    /**
     * Attempts to append <tt>c</tt> until the line gets the length <tt>
     * newLength</tt>. If the line is already longer than <tt>newLength</tt>, 
     * then the internal strings is cut to <tt>newLength</tt>.
     * @param newLength New length of the last line.
     * @param c Character to append.
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder fillOrCut(int newLength, char c) {
        if (newLength < 0) {
            throw new IllegalArgumentException();
        }
        int lineStart = _delegate.lastIndexOf(NL);
        if (lineStart == -1) {
            lineStart = 0;
        } else {
            lineStart += NL.length();
        }
        int lineLen = _delegate.length() - lineStart;

        if (newLength < lineLen) {
            _delegate = new StringBuilder(_delegate.substring(0, lineStart + newLength));
            return this;
        }
        return fill(newLength, c);
    }
    
    /**
     * Attempts to append <tt>c</tt> until the buffer gets the length <tt>
     * newLength</tt>. If the buffer is already longer than <tt>newLength</tt>, 
     * then the internal strings is cut to <tt>newLength</tt>.
     * @param newLength New length of the returned buffer.
     * @param c Character to append.
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder fillOrCutBuffer(int newLength, char c) {
        if (newLength < _delegate.length()) {
            _delegate = new StringBuilder(_delegate.substring(0, newLength));
            return this;
        }
        for (int i = _delegate.length(); i < newLength; i++ ) {
            _delegate.append(c);
        }
        return this;
    }
    
    /**
     * @return length of the internal builder.
     */
    public int length() {
        return _delegate.length();
    }

    /**
     * Inserts the string into this character sequence.
     *
     * @param offset the offset
     * @param str the string
     * @return The updated instance of FormattedStringBuilder.
     */
    public FormattedStringBuilder insert(int offset, String str) {
        _delegate.insert(offset, str);
        return this;
    }
    
    /**
     * Creates a new instance that is wrapped around a given <tt>StringBuilder
     * </tt>.
     * @param builder A StringBuilder
     * @return the new instance of <tt>FormattedStringBuilder</tt>.
     */
    public static FormattedStringBuilder wrap(StringBuilder builder) {
        FormattedStringBuilder b = new FormattedStringBuilder();
        b._delegate = builder;
        return b;
    }
    
    /**
     * @see java.lang.Object#toString()
     */
    @Override
	public String toString() {
        return _delegate.toString();
    }
}
