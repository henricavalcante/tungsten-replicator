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
 * Initial developer(s):
 * Contributor(s):
 */

package com.continuent.tungsten.common.mysql;

public abstract class MySQLGreetingPacket
{
    /**
     * Extracts the seed encoded in this packet.<br>
     * The seed is the concatenation of two arrays of bytes found in this
     * packet.
     * 
     * @return the server seed to salt password with for password encoding
     *         purposes
     */
    public static byte[] getSeed(MySQLPacket p)
    {
        // discard what we don't need, get the seed
        p.getByte();
        p.getString();
        p.getInt32();
        // here is the first part of the seed
        byte[] seed1 = p.getBytes(8);
        p.getByte();
        p.getShort();
        p.getByte();
        p.getShort();
        p.getBytes(13);
        // here is the second part
        byte[] seed2 = p.getBytes(12);
        // construct the final seed
        byte[] finalSeed = new byte[seed1.length + seed2.length];
        System.arraycopy(seed1, 0, finalSeed, 0, seed1.length);
        System.arraycopy(seed2, 0, finalSeed, seed1.length, seed2.length);
        return finalSeed;
    }
}
