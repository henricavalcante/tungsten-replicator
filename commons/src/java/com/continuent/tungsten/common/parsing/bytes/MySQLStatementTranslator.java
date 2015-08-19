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

import java.io.UnsupportedEncodingException;

/**
 * Utility class to translate MySQL statements from a native charset to Java
 * Unicode strings accounting for introducers for binary and alternative
 * character sets. Syntax for MySQL strings is described in <a
 * href="http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html"> MySQL
 * online documentation</a>. A typical example looks like the following:
 * <p>
 * <code><pre>
 * INSERT INTO `storage'_binary'` VALUES (25, 'col_binary', _binary'\0\0\0\0')
 * </pre></code> This string illustrates some of the potential ambiguities in
 * string translation. To avoid confusion we implement a full tokenizer that
 * ignores data embedded in normal strings or comments. We thus translate the
 * preceding string into the following, where binary data are replaced by a
 * hexadecimal string format:
 * <p>
 * <code><pre>
 * INSERT INTO `storage'_binary'` VALUES (25, 'col_binary', _binary x'00000000')
 * </pre></code> The translation is based on state machines according to the
 * following principles.
 * <p>
 * <ul>
 * <li>Binary and alternative charset strings denoted by introducers of the form
 * _&lt;introducer&gt;'value' or _&lt;introducer&gt;"value" are converted to hex
 * strings that can translate safely to Unicode.</li>
 * <li>All bytes outside of such strings are translated using the character set
 * assigned when creating the MySQLStatementTranslator instance.</li>
 * <li>Values within ordinary strings starting with backtick (`), single, and
 * double quotes excluded from parsing for introducers. The same applies to
 * values within comments.</li>
 * </ul>
 * Performance is an important consideration in the translation algorithm as
 * binary strings in particular are potentially quite large. The parsing and
 * translation processing uses pointers into the byte string to minimize object
 * creation. The translation values for byte strings are pre-computed strings.
 * The performance overhead of parsing + translation is about 10% over unparsed
 * string translation.
 * <p>
 * Finally, it should be noted that translation into the safe hex format doubles
 * the size of binary strings. Users should expect to double memory allocations
 * accordingly, including MySQL specific settings like max_packet_size, which
 * sets the maximum size of a single client request.
 * 
 * @author <a href="mailto:robert.hodges@continuent.com">Robert Hodges</a>
 * @version 1.0
 */
public class MySQLStatementTranslator
{
    // Character set names used for introducers.
    private static String               charsetNames[]         = {"armscii8",
            "ascii", "big5", "binary", "cp1250", "cp1251", "cp1256", "cp1257",
            "cp850", "cp852", "cp866", "cp932", "dec8", "eucjpms", "euckr",
            "gb2312", "gbk", "geostd8", "greek", "hebrew", "hp8", "keybcs2",
            "koi8r", "koi8u", "latin1", "latin2", "latin5", "latin7", "macce",
            "macroman", "sjis", "swe7", "tis620", "ucs2", "ujis", "utf8"};

    // Character string and comment fragments.
    private static final String         UNDERSCORE             = "_";
    private static final String         SINGLE_QUOTE           = "'";
    private static final String         DOUBLE_QUOTE           = "\"";
    private static final String         BACKTICK               = "`";
    private static final String         ESCAPE                 = "\\";
    private static final String         COMMENT_START          = "/*";
    private static final String         COMMENT_END            = "*/";

    // Tokens
    private static final int            TOK_INTRO_SINGLE_QUOTE = 1;
    private static final int            TOK_INTRO_DOUBLE_QUOTE = 2;
    private static final int            TOK_SINGLE_QUOTE       = 3;
    private static final int            TOK_DOUBLE_QUOTE       = 4;
    private static final int            TOK_BACKTICK           = 5;
    private static final int            TOK_COMMENT_START      = 6;
    private static final int            TOK_COMMENT_END        = 7;

    // Charset state machines.
    private String                      charset;
    private ByteTranslationStateMachine textFsm;
    private ByteTranslationStateMachine embeddedStringFsm;
    private ByteTranslationStateMachine normalStringFsm;
    private ByteTranslationStateMachine commentFsm;

    // Escape characters.
    private byte                        char_0;
    private byte                        char_b;
    private byte                        char_n;
    private byte                        char_r;
    private byte                        char_t;
    private byte                        char_Z;

    // Character lengths.
    private int                         singleQuoteLength;
    private int                         doubleQuoteLength;

    // Translation table for byte values.
    private String[]                    byteXlationTable       = new String[256];

    public MySQLStatementTranslator(String charset)
            throws UnsupportedEncodingException
    {
        this.charset = charset;
        setup();
    }

    // Set up state machines.
    private void setup() throws UnsupportedEncodingException
    {
        // Set up text translation machine with single and double quote
        // introducers, start characters for normal strings, and comment
        // start sequence.
        textFsm = new ByteTranslationStateMachine();

        for (String charsetName : charsetNames)
        {
            // Add single and double quote versions of next introducer.
            String introSingleQuote = UNDERSCORE + charsetName + SINGLE_QUOTE;
            String introDoubleQuote = UNDERSCORE + charsetName + DOUBLE_QUOTE;
            String substitute = UNDERSCORE + charsetName;

            textFsm.load(introSingleQuote.getBytes(charset),
                    TOK_INTRO_SINGLE_QUOTE, substitute.getBytes(charset), false);
            textFsm.load(introDoubleQuote.getBytes(charset),
                    TOK_INTRO_DOUBLE_QUOTE, substitute.getBytes(charset), false);
        }

        textFsm.load(SINGLE_QUOTE.getBytes(charset), TOK_SINGLE_QUOTE, null,
                false);
        textFsm.load(DOUBLE_QUOTE.getBytes(charset), TOK_DOUBLE_QUOTE, null,
                false);
        textFsm.load(BACKTICK.getBytes(charset), TOK_BACKTICK, null, false);
        textFsm.load(COMMENT_START.getBytes(charset), TOK_COMMENT_START, null,
                false);

        // Set up embedded string state machine with quote characters and
        // escape sequences.
        embeddedStringFsm = new ByteTranslationStateMachine();
        embeddedStringFsm.load(SINGLE_QUOTE.getBytes(charset),
                TOK_SINGLE_QUOTE, SINGLE_QUOTE.getBytes(charset), false);
        embeddedStringFsm.load(DOUBLE_QUOTE.getBytes(charset),
                TOK_DOUBLE_QUOTE, DOUBLE_QUOTE.getBytes(charset), false);
        embeddedStringFsm.load(ESCAPE.getBytes(charset), -1, null, true);

        // Set up state machine for normal strings.
        normalStringFsm = new ByteTranslationStateMachine();
        normalStringFsm.load(SINGLE_QUOTE.getBytes(charset), TOK_SINGLE_QUOTE,
                null, false);
        normalStringFsm.load(DOUBLE_QUOTE.getBytes(charset), TOK_DOUBLE_QUOTE,
                null, false);
        normalStringFsm.load(BACKTICK.getBytes(charset), TOK_BACKTICK, null,
                false);
        normalStringFsm.load(ESCAPE.getBytes(charset), -1, null, true);

        commentFsm = new ByteTranslationStateMachine();
        commentFsm.load(COMMENT_END.getBytes(charset), TOK_COMMENT_END, null,
                false);
        commentFsm.load(ESCAPE.getBytes(charset), -1, null, true);

        // Following are standard escape sequences for embedded strings.
        char_0 = toSingleByte(charset, "0");
        char_b = toSingleByte(charset, "b");
        char_n = toSingleByte(charset, "n");
        char_r = toSingleByte(charset, "r");
        char_t = toSingleByte(charset, "t");
        char_Z = toSingleByte(charset, "Z");

        // Populate byte string translation table.
        for (int i = 0; i < 256; i++)
        {
            byte b = (byte) i;
            byteXlationTable[i] = String.format("%02X", b);
        }

        // Compute lengths of single and double quote byte strings in current
        // charset.
        singleQuoteLength = new String(SINGLE_QUOTE).getBytes(charset).length;
        doubleQuoteLength = new String(DOUBLE_QUOTE).getBytes(charset).length;
    }

    public String toJavaString(byte[] bytes, int offset, int length)
            throws UnsupportedEncodingException
    {
        // Set up translation buffer data.
        CharacterTranslationBuffer ctb = new CharacterTranslationBuffer();
        ctb.load(bytes, offset, length, charset);
        textFsm.init();

        // Loop.
        while (ctb.hasNext())
        {
            ByteState state = textFsm.add(ctb.next());
            if (state == ByteState.ACCEPTED)
            {
                int token = textFsm.getToken();
                switch (token)
                {
                    case TOK_INTRO_SINGLE_QUOTE :
                        // Translate up to single quote char and process
                        // embedded string.
                        ctb.translateAndAppendPending(singleQuoteLength);
                        processEmbeddedString(ctb, embeddedStringFsm, charset,
                                TOK_SINGLE_QUOTE);
                        break;
                    case TOK_INTRO_DOUBLE_QUOTE :
                        // Translate up to double quote char and process
                        // embedded string.
                        ctb.translateAndAppendPending(doubleQuoteLength);
                        processEmbeddedString(ctb, embeddedStringFsm, charset,
                                TOK_DOUBLE_QUOTE);
                        break;
                    case TOK_SINGLE_QUOTE :
                    case TOK_DOUBLE_QUOTE :
                    case TOK_BACKTICK :
                        ctb.translateAndAppendPending(0);
                        processNormalString(ctb, normalStringFsm, charset,
                                token);
                        break;
                    case TOK_COMMENT_START :
                        ctb.translateAndAppendPending(0);
                        processComment(ctb, commentFsm, charset,
                                TOK_COMMENT_END);
                        break;
                }
            }
        }
        ctb.translateAndAppendPending(0);

        // Return the translated output.
        return ctb.getOutput();
    }

    // Process an embedded string, handling escape characters and correctly
    // recognizing the terminating string.
    private void processEmbeddedString(CharacterTranslationBuffer ctb,
            ByteTranslationStateMachine stringFsm, String charset,
            int terminatingToken) throws UnsupportedEncodingException
    {
        stringFsm.init();
        ctb.append(" x'");
        while (ctb.hasNext())
        {
            byte c = ctb.next();
            ByteState state = stringFsm.add(c);
            if (state == ByteState.ACCEPTED)
            {
                int token = stringFsm.getToken();
                if (token == terminatingToken)
                {
                    ctb.appendAndClearPending(new String(stringFsm
                            .getSubstitute(), charset));
                    break;
                }
                else if (stringFsm.isSubstitute())
                {
                    for (byte b : stringFsm.getSubstitute())
                        ctb.appendAndClearPending(byteToHexString(b));
                }
                else
                {
                    ctb.appendAndClearPending(byteToHexString(c));
                }
            }
            else if (state == ByteState.ESCAPE)
            {
                // Process escape sequences for MySQL.
                String escapedValue;
                if (c == char_0)
                    escapedValue = "00"; // null;
                else if (c == char_b)
                    escapedValue = "08"; // backspace
                else if (c == char_n)
                    escapedValue = "0A"; // new line
                else if (c == char_r)
                    escapedValue = "0D"; // carriage return
                else if (c == char_t)
                    escapedValue = "09"; // tab
                else if (c == char_Z)
                    escapedValue = "1A"; // ^Z, means EOF on Windows
                else
                    escapedValue = byteToHexString(c);
                ctb.appendAndClearPending(escapedValue);
            }
        }
    }

    // Process a normal string or comment. We just ignore all characters
    // through the end of the string.
    private void processNormalString(CharacterTranslationBuffer ctb,
            ByteTranslationStateMachine normalStringFsm, String charset,
            int terminatingToken)
    {
        normalStringFsm.init();
        while (ctb.hasNext())
        {
            byte c = ctb.next();
            ByteState state = normalStringFsm.add(c);
            if (state == ByteState.ACCEPTED
                    && normalStringFsm.getToken() == terminatingToken)
            {
                break;
            }
        }
    }

    // Process a comment. We just ignore all characters
    // through the end of the comment.
    private void processComment(CharacterTranslationBuffer ctb,
            ByteTranslationStateMachine commentFsm, String charset,
            int terminatingToken)
    {
        commentFsm.init();
        while (ctb.hasNext())
        {
            byte c = ctb.next();
            ByteState state = commentFsm.add(c);
            if (state == ByteState.ACCEPTED
                    && commentFsm.getToken() == terminatingToken)
            {
                break;
            }
        }
    }

    // Translate a byte to hex representation.
    private String byteToHexString(byte b)
    {
        return byteXlationTable[(int) b & 0xFF];
    }

    // Convert String to single byte.
    private byte toSingleByte(String charset, String c)
            throws UnsupportedEncodingException
    {
        byte[] bytes = c.getBytes(charset);
        if (bytes.length > 1)
            throw new UnsupportedEncodingException(
                    "Escape character must be single byte: " + c);
        else
            return bytes[0];
    }
}
