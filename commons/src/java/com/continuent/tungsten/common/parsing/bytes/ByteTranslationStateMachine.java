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
 * Contributor(s): 
 */

package com.continuent.tungsten.common.parsing.bytes;

/**
 * Implements a finite state machine for byte string translation. The state
 * machine uses a trie structure to track strings that have special meanings.
 * The {@link #add(byte)} method inserts a new byte "event" into the state
 * machine for processing.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class ByteTranslationStateMachine
{
    // Link in a trie of byte strings.
    class ByteTrie
    {
        byte       value;
        ByteState  state = ByteState.BUFFERING;
        int        token = -1;
        byte[]     substitute;
        boolean    escape;
        ByteTrie[] links = new ByteTrie[256];

        ByteTrie(byte value, ByteState state, int token, byte[] substitute,
                boolean escape)
        {
            this.value = value;
            this.state = state;
            this.token = token;
            this.escape = escape;
            this.substitute = substitute;
        }

        ByteTrie(byte value)
        {
            this.value = value;
        }
    }

    // Head of state machine. This is a dummy entry.
    ByteTrie head         = new ByteTrie((byte) 0x00, ByteState.NONE, -1, null,
                                  false);

    // Current location in state machine as we walk the trie.
    ByteTrie current;
    // Last value accepted.
    ByteTrie lastAccepted = head;
    // True if last byte processed was an escape character.
    boolean  escape;

    public ByteTranslationStateMachine()
    {
    }

    /**
     * Initializes the state machine. Must be the first call to the state
     * machine.
     */
    public void init()
    {
        current = head;
        escape = false;
    }

    /**
     * Loads a string sequence into the state machine.
     * 
     * @param value Byte array containing sequence
     * @param token If the string is a token value, a constant equal to or
     *            greater than zero to identify the token
     * @param substitute An alternative value that is a substitute that should
     *            be accepted instead of the parsed string
     * @param escape True if this represents an escape sequence. The next byte
     *            after the escape sequence is accepted.
     */
    public void load(byte[] value, int token, byte[] substitute, boolean escape)
    {
        ByteTrie bt = head;
        for (int i = 0; i < value.length; i++)
        {
            int index = value[i] & 0xFF;
            ByteTrie existing = bt.links[index];
            if (i == value.length - 1)
            {
                // We are at the end.
                if (existing == null)
                {
                    if (escape)
                    {
                        bt.links[index] = new ByteTrie(value[i],
                                ByteState.BUFFERING, token, substitute, escape);
                    }
                    else
                    {
                        bt.links[index] = new ByteTrie(value[i],
                                ByteState.ACCEPTED, token, substitute, escape);
                    }
                }
                else
                {
                    // We don't have unique leaf node, which means the structure
                    // is ambiguous.
                    throw new UnsupportedOperationException(
                            "Byte string would result in an ambiguous byte state machine: "
                                    + new String(value));
                }
            }
            else
            {
                if (existing == null)
                {
                    bt.links[index] = new ByteTrie(value[i]);
                }
            }

            // We either found or have created the next entry. Get it now.
            bt = bt.links[index];
        }
    }

    /**
     * Add a byte to the state machine and return the corresponding state.
     * Clients should fetch substitute strings and tokens after this call
     * occurs.
     */
    public ByteState add(byte b)
    {
        lastAccepted = head;
        ByteState state = ByteState.NONE;
        if (escape)
        {
            // Previous value was an escape character.
            state = ByteState.ESCAPE;
            escape = false;
            current = head;
        }
        else
        {
            int index = b & 0xFF;
            ByteTrie next = current.links[index];
            if (next == null)
            {
                // This is an ordinary character.
                state = ByteState.ACCEPTED;
                current = head;
            }
            else
            {
                // We are processing a string of 1 or more characters.
                state = next.state;
                lastAccepted = next;
                if (next.escape)
                {
                    current = next;
                    escape = true;
                }
                else if (state == ByteState.ACCEPTED)
                {
                    current = head;
                }
                else
                {
                    current = next;
                }
            }
        }
        return state;
    }

    /**
     * Return true if a substitute string is offered.
     */
    public boolean isSubstitute()
    {
        return (lastAccepted.substitute != null);
    }

    /** Returns the last string substition. */
    public byte[] getSubstitute()
    {
        return lastAccepted.substitute;
    }

    /** Returns true if the last accepted string is a token. */
    public boolean isToken()
    {
        return lastAccepted.token >= 0;
    }

    /** Returns the last token value. */
    public int getToken()
    {
        return lastAccepted.token;
    }
}