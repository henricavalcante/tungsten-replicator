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
 * Initial developer(s): Scott Martin
 * Contributor(s): Stephane Giron 
 */

package com.continuent.tungsten.replicator.extractor.oracle;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.ListIterator;
import java.lang.String;
import java.text.SimpleDateFormat;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.database.Column;
import com.continuent.tungsten.replicator.database.Database;
import com.continuent.tungsten.replicator.database.Table;
import com.continuent.tungsten.replicator.database.AdditionalTypes;
import com.continuent.tungsten.replicator.dbms.OneRowChange;
import com.continuent.tungsten.replicator.dbms.RowChangeData;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnSpec;
import com.continuent.tungsten.replicator.dbms.OneRowChange.ColumnVal;

/**
 * Defines a class that can parse row level changes returned by Oracle into java
 * classes appropriate for replication. An example string that this class might
 * be called upon to parse is
 * 0009.000b.0000057b:069d.00001161.0010:U:000016279:AAAD+XAAEAAABZrAAA:
 * 0024000200018002c1020003000cScott Martin:0003000100028003c26464 In general
 * terms this represents an update "U", to object # "000016279", transaction id
 * "0009.000b.0000057b", rowid = "AAAD+XAAEAAABZrAAA". INSERT INTO BRANCH
 * VALUES(1, 1, 'Scott');
 * 0003.0001.00000515:01d4.00000962.0070:I:000011001:AAACr5AAEAAAAAdAAE:
 * 0000:00038002c1068002c1060005Scott UPDATE BRANCH SET BALANCE = 0 WHERE BNO =
 * 10 0001.0017.0000050e:01d4.00000974.014c:U:000011001:AAACr5AAEAAAAAdAAJ:
 * 0029000300018002c10b00030005Scott00028002c10b:000300010002800180
 * 0003.0012.0000051a:01d6.00000069.0010:U:000011001:AAACsDAAEAAAAAdAAJ:
 * 001d000200018002c10b00030005Scott:000300010002800180 DELETE FROM BRANCH WHERE
 * BNO = 10
 * 0002.0015.00000510:01d4.0000097b.0010:D:000011001:AAACr5AAEAAAAAdAAJ:
 * 0027000300018002c10b000280018000030005Scott:
 * 0002.0027.00000514:01d6.0000006b.0010:D:000011001:AAACsDAAEAAAAAdAAJ:
 * 0010000100018002c10b:
 * 
 * @author <a href="mailto:scott.martin@continuent.com">Scott Martin</a>
 * @author <a href="mailto:stephane.giron@continuent.com">Stephane Giron</a>
 */
public class OracleParser
{

    private static Logger       logger     = Logger
                                                   .getLogger(OracleExtractor.class);
    private static final String NULL_VALUE = "NULL"; 
    
    
    private String            parseString;
    private int               parseLocation;
    private boolean           parseHexMode;
    private long              parseLastLength;
    private int               parseLastColumnNumber;
    private String            parseLastColumnData;
    private Table             parseLastTable;
    private Column            parseLastColumn;
    private Serializable      parseLastDate;
    private ArrayList<Map>    dictionary;
    private Database          database;

    OneRowChange              rowEvent;

    class Map
    {
        int   objectNumber;
        Table table;
    }

    /**
     * Creates a new <code>OracleParser</code> object
     */
    public OracleParser(Database database)
    {
        Column c1 = new Column("TEST_PK",   java.sql.Types.INTEGER);
        Column c2 = new Column("CHAR_COL",  java.sql.Types.VARCHAR, 100);
        Column c3 = new Column("CHAR_COL2", java.sql.Types.VARCHAR, 100);

        Table t = new Table("TEST", "TESTINSRANDWITHOUT");

        dictionary = new ArrayList<Map>();

        t.AddColumn(c1);
        t.AddColumn(c2);
        t.AddColumn(c3);

        newTable(t, 13308);

        this.database = database;
    }

    private Table findTable(int objectNumber, String lastSCN)
            throws OracleExtractException
    {
        Table t;

        // we should be using hash lookup here...
        if (lastSCN == null)
        {
            // Try to check in the cache (not yet implemented - cache with SCN)
            ListIterator<Map> litr = dictionary.listIterator();

            for (; litr.hasNext();)
            {
                Map map = litr.next();
                if (map.objectNumber == objectNumber)
                    return map.table;
            }
        }
        try
        {
            t = database.findTable(objectNumber, lastSCN);
            if (t == null)
            {
                logger.warn("Cannot find object number " + objectNumber
                        + " in database");
                throw new OracleExtractException("Cannot find object number "
                        + Integer.toString(objectNumber));
            }
            else
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug("Found obj " + objectNumber
                            + " in database, storing in cache");
                }
                newTable(t, objectNumber);
            }
        }
        catch (SQLException e)
        {
            throw new OracleExtractException(e);
        }

        return t;
    }

    private void newTable(Table t, int objectNumber)
    {
        Map map = new Map();

        map.objectNumber = objectNumber;
        map.table = t;

        dictionary.add(map);
    }

    private long hexVal8()
    {
        long val = 0;

        parseHexMode = false;

        val = java.lang.Long.parseLong(parseString.substring(parseLocation,
                parseLocation + 8), 16);

        /*
         * if length , when treated as 4 byte value, has high order bit on, the
         * resulting data will be listed as two byte hex representation, else
         * one byte "as is". examples 000cScott Martin -> string "Scott Martin"
         * 8002c102 -> the 2 character string 0xc1 concatted with 0x02.
         */
        if (val >= 0x80000000)
        {
            val -= 0x80000000;
            parseHexMode = true;
        }
        parseLastLength = val;

        parseLocation += 8;
        return val;
    }
    
    private int hexVal4()
    {
        int val = 0;

        parseHexMode = false;

        val = java.lang.Integer.parseInt(parseString.substring(parseLocation,
                parseLocation + 4), 16);

        /*
         * if length , when treated as 4 byte value, has high order bit on, the
         * resulting data will be listed as two byte hex representation, else
         * one byte "as is". examples 000cScott Martin -> string "Scott Martin"
         * 8002c102 -> the 2 character string 0xc1 concatted with 0x02.
         */
        if (val >= 0x8000)
        {
            val -= 0x8000;
            parseHexMode = true;
        }
        parseLastLength = val;

        parseLocation += 4;
        return val;
    }

    private int hexVal2(String s)
    {
        int val = 0;
        val = java.lang.Integer.parseInt(s, 16);
        return val;
    }

    private String parseColon()
    {
        int start = parseLocation;
        int end;

        end = parseString.indexOf(':', parseLocation);
        if (end == -1)
            end = parseString.length();

        parseLocation += end - start + 1;
        return parseString.substring(start, end);
    }

    /*
     * length expected in parseLastLength
     */
    private String parseData()
    {
        long len = parseLastLength;
        int start = parseLocation;

        if (parseHexMode)
            len *= 2;
        parseLocation += len;

        return parseString.substring(start, parseLocation);
    }

    private ArrayList<Integer> parseToInts(String s)
    {
        ArrayList<Integer> al = new ArrayList<Integer>();
        int i;

        for (i = 0; i < s.length(); i += 2)
        {
            al.add(hexVal2(s.substring(i, i + 2)));
        }
        return al;
    }

    private byte[] parseToBytes(String s)
    {
        byte[] ba = new byte[s.length() / 2];

        int i;
        int val;

        for (i = 0; i < s.length(); i += 2)
        {
            val = hexVal2(s.substring(i, i + 2));
            ba[i / 2] = (byte) val;
        }
        return ba;
    }

    private String parseToString(String s)
    {
        byte[] ba = parseToBytes(s);

        return new String(ba);
    }
    
    private String parseToStringUTF16(String s)
    {
        byte[] ba = parseToBytes(s);
        String retval;

        try
        {
            retval = new String(ba, "UTF-16");
        } 
        catch (UnsupportedEncodingException e)
        {
            retval = "";
        }
        
        return retval;
    }


    /*
     * Layout of an Oracle date: Byte: Value: ----- ------- 0 Century
     * (excess-100 notation) 1 Year (excess-100 notation) 2 Month 3 Day 4 Hour 5
     * Minute 6 Second current format is MM/DD/YYYY HH24:MI:SS example is
     * 08/06/1965 14:02:27 for August 6th 1965 at aproximately 2 in the
     * afternoon. 
     * <p/>
     * TODO: return/invent generic date class to pass through to
     * applier
     */
    private String parseOracleDateToString(String s)
            throws OracleExtractException
    {
        String retval = "";
        int century;
        int year;
        int month;
        int day;
        int hour;
        int minute;
        int second;
        int length;
        ArrayList<Integer> al;

        if (s.length() == 0)
        {
            return NULL_VALUE;
        }
        
        /* quick conversion array of numbers 0..99 to strings */
        String dateStrings[] = {"00", "01", "02", "03", "04", "05", "06", "07",
                "08", "09", "10", "11", "12", "13", "14", "15", "16", "17",
                "18", "19", "20", "21", "22", "23", "24", "25", "26", "27",
                "28", "29", "30", "31", "32", "33", "34", "35", "36", "37",
                "38", "39", "40", "41", "42", "43", "44", "45", "46", "47",
                "48", "49", "50", "51", "52", "53", "54", "55", "56", "57",
                "58", "59", "60", "61", "62", "63", "64", "65", "66", "67",
                "68", "69", "70", "71", "72", "73", "74", "75", "76", "77",
                "78", "79", "80", "81", "82", "83", "84", "85", "86", "87",
                "88", "89", "90", "91", "92", "93", "94", "95", "96", "97",
                "98", "99",};

        al = parseToInts(s);
        length = al.size();

        if (length != 7)
            throw new OracleExtractException("bad oracle date");

        century = al.get(0);
        year = al.get(1);
        month = al.get(2);
        day = al.get(3);
        hour = al.get(4) - 1;
        minute = al.get(5) - 1;
        second = al.get(6) - 1;

        /* determine if this is a BC year */
        if ((century < 100) || (year < 100))
        {
            /* compute the actual century/year */
            century = 100 - century;
            year = 100 - year;

            /* add the BC indication to the output buffer */
            retval += "BC ";
        }
        else
        /* not a BC year */
        {
            /* compute the actual century/year */
            century -= 100;
            year -= 100;
            retval += "AD ";
        }
        
        SimpleDateFormat format = new SimpleDateFormat("G yyyy-MM-dd HH:mm:ss");

        retval += dateStrings[century];
        retval += dateStrings[year];
        retval += "-";
        retval += dateStrings[month];
        retval += "-";
        retval += dateStrings[day];
        retval += " ";

        retval += dateStrings[hour];
        retval += ":";
        retval += dateStrings[minute];
        retval += ":";
        retval += dateStrings[second];
        
        //parseLastDate = java.sql.Timestamp.valueOf(retval);
        try
        {
            //parseLastDate = format.parse(retval);
            parseLastDate = (format.parse(retval).getTime());
        }
        catch (Exception e)
        {
            throw new OracleExtractException("Bad date " + e);
        }

        return retval;
    }

    /*
     * The following comment was written by me 15 years ago and is stolen from
     * the C based implementation in tgdc.c. I suspect the veracity of the
     * comment is as true now as it was then. SCott. Number to (2) String:
     * Returns length of formatted number string, or zero if output buffer is
     * too small. The most important feature of Oracle's representation of
     * Oracle numbers is that smaller numbers "sort" lower than larger numbers
     * when doing a simple string compare. That is if A is smaller than B, the
     * Oracle_rep(A) is smaller than the Oracle_rep(B). With this in mind, it is
     * fairly straight forward to understand Oracle's representation of numbers.
     * Oracle numbers are of the form EXP D<n> .... D<0> The EXP byte is the
     * exponent byte and the D<n> bytes are base 100 digits in the number. The
     * EXP byte is divided into the sign bit (0x80) and the remaining exponent
     * bits (0x7f). if the sign bit is on the number is positive else negative.
     * The remaining exp bits are the exponent + 65. For example if EXP = C2
     * (1100 0000 0000 0010), the number is positive and has an exponent of 1
     * which equals (65 - (EXP & 0x7f)). so in short exponent = (65 - (EXP &
     * 0x7f)) sign = EXP & 0x80 NOTE: Negative numbers are a bit more
     * complicated and not discussed here. now for the digits. Each byte
     * represents TWO decimal digits. The two decimal digits are simply D<n>.
     * In other words 0x46 is simply "70" - the value of 0x46. So let us look at
     * a few examples Oracle Real life Why? --------- ----------
     * --------------------- 0x80 0 0xc202 2 exp = 65 - (0x7f & 0xc2) = 1 : 0x02 =
     * 2 : 2 X 100^1 0xc302 200 exp = 65 - (0x7f & 0xc3) = 2 : 0x02 = 2 : 2 X
     * 100^2 0xc30202 202 exp = 65 - (0x7f & 0xc3) = 2 : 0x02 = 2 : 202 X 100^1
     * Before I poorly explain this concept any further, I better let the code
     * speak for itself. It is more precise....
     */

    private String parseOracleNumberToString(String s)
    {
        int position = 0;
        int endPosition;
        int exponent;
        int length;
        ArrayList<Integer> al;
        Integer val = 0;
        boolean positive;
        String retval = "";
        boolean afterDecimal = false;
        int ndisp;
        int digit;
        /* quick lookup array of 0..99 / 10 converted to a character */
        String ddivar[] = {"0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
                "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "2", "2",
                "2", "2", "2", "2", "2", "2", "2", "2", "3", "3", "3", "3",
                "3", "3", "3", "3", "3", "3", "4", "4", "4", "4", "4", "4",
                "4", "4", "4", "4", "5", "5", "5", "5", "5", "5", "5", "5",
                "5", "5", "6", "6", "6", "6", "6", "6", "6", "6", "6", "6",
                "7", "7", "7", "7", "7", "7", "7", "7", "7", "7", "8", "8",
                "8", "8", "8", "8", "8", "8", "8", "8", "9", "9", "9", "9",
                "9", "9", "9", "9", "9", "9"};
        /* quick lookup array of 0..99 % 10 converted to a character */
        String dmodar[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1",
                "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2", "3",
                "4", "5", "6", "7", "8", "9", "0", "1", "2", "3", "4", "5",
                "6", "7", "8", "9", "0", "1", "2", "3", "4", "5", "6", "7",
                "8", "9", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
                "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "1",
                "2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2", "3",
                "4", "5", "6", "7", "8", "9"};
        String ddiv10;
        String dmod10;

        if (s.length() == 0)
            return NULL_VALUE;

        al = parseToInts(s);

        length = al.size();

        val = al.get(0);

        /* zero is special case */
        if (val == 0x80)
            return "0";

        if ((val & 0x80) != 0)
        {
            positive = true;
            endPosition = length - 1;
            exponent = val & 0x7f;
        }
        else
        {
            positive = false;
            /*
             * Negative numbers have a trailing "102" unless the number is 20
             * bytes long. Conditionally remove the "102".
             */
            endPosition = length - 2;
            if ((endPosition == 19) && (al.get(20) != 102))
                endPosition++;
            retval += "-";
            exponent = ~val & 0x7f;
        }
        exponent -= 65;

        if (exponent < 0)
        {
            retval += "0.";
            afterDecimal = true;
            for (ndisp = 0; ndisp < -exponent - 1; ndisp++)
            {
                retval += "00";
            }
        }

        for (position = 0, ndisp = 0; position < endPosition; position++, ndisp += 2)
        {
            val = al.get(position + 1);

            /* Check for the need of intermediate decimal */
            if (position != 0 && (position == (exponent + 1)))
            {
                retval += ".";
                afterDecimal = true;
            }

            if (positive)
                digit = val - 1;
            else
                digit = 101 - val;

            if (digit >= 100)
            {
                System.out.format("Bad Oracle number \"%s\"\n", s);
                return "0";
            }

            ddiv10 = ddivar[digit];
            dmod10 = dmodar[digit];

            if (!((exponent >= 0) && (ndisp == 0) && ddiv10.equals("0")))
            {
                retval += ddiv10;
            }

            /* No trialing zeros on last digit if beyond decimal point */
            if (!(afterDecimal && (position == endPosition - 1) && dmod10
                    .equals("0")))
            {
                retval += dmod10;
            }
        }

        /* Check for the need for trailing zeros... */
        if (exponent >= 0)
        {
            for (; endPosition <= exponent; endPosition++)
            {
                retval += "00";
            }
        }

        return retval;
    }

    /**
     * Converts the byte array into a 32 bit integer, which is IEEE 754 compliant.
     * Conversion is done as follows : if the first bit is 1, this will be
     * interpreted as a positive number, just replacing this first bit by 0
     * (which indicates a positive number in IEEE754). If the first bit is 0,
     * then the result will be the ones complement of the byte array.
     * 
     * @param data
     */
    private int convertBinaryFloatToInt(byte[] data)
    {
        if (data == null || data.length != 4)
            return 0x0;
        int value;
        if ((0x80 & data[0]) == 0x80)
        {
            value = ((0x7f & data[0]) << 24 | (0xff & data[1]) << 16
                    | (0xff & data[2]) << 8 | (0xff & data[3]));
        }
        else
        {
            value = ~(((0xff & data[0]) << 24 | (0xff & data[1]) << 16
                    | (0xff & data[2]) << 8 | (0xff & data[3])));
        }
        return value;
    }

    /**
     * Converts the byte array into a 64 bit long, which is IEEE 754 compliant.
     * Conversion is done as follows : if the first bit is 1, this will be
     * interpreted as a positive number, just replacing this first bit by 0
     * (which indicates a positive number in IEEE754). If the first bit is 0,
     * then the result will be the ones complement of the byte array.
     * 
     * @param data
     */
    private long convertBinaryDoubleToLong(byte[] data)
    {
        if (data == null || data.length != 8)
            return 0x0;

        long value;
        if ((0x80 & data[0]) == 0x80)
        {
            value = ((long) (0x7f & data[0]) << 56
                    | (long) (0xff & data[1]) << 48
                    | (long) (0xff & data[2]) << 40
                    | (long) (0xff & data[3]) << 32
                    | (long) (0xff & data[4]) << 24
                    | (long) (0xff & data[5]) << 16
                    | (long) (0xff & data[6]) << 8
                    | (long) (0xff & data[7]));
        }
        else
        {
            value = ~((long) (0xff & data[0]) << 56
                    | (long) (0xff & data[1]) << 48
                    | (long) (0xff & data[2]) << 40
                    | (long) (0xff & data[3]) << 32
                    | (long) (0xff & data[4]) << 24
                    | (long) (0xff & data[5]) << 16
                    | (long) (0xff & data[6]) << 8
                    | (long) (0xff & data[7]));
        }
        return value;
    }

    /**
     * Convert the byte arrary into a 32 bit float
     * 
     * @param data
     */
    private float toFloat(byte[] data)
    {
        if (data == null || data.length != 4)
            return 0;
        return Float.intBitsToFloat(convertBinaryFloatToInt(data));
    }

    /**
     * Convert the byte array into a 64 bit double
     * 
     * @param data
     */
    private double toDouble(byte[] data)
    {
        if (data == null || data.length != 8)
            return 0;
        return Double.longBitsToDouble(convertBinaryDoubleToLong(data));
    }

    /**
     * convertHexToInt converts a given hexadecimal string to an integer value.
     * 
     * @param hexValue the string that is to be converted
     * @return the decimal value corresponding to the given hexadecimal value, or 0 if the
     *         string cannot be converted to integer
     */
    private int convertHexToInt(String hexValue)
    {
        int value;
        try
        {
            value = Integer.parseInt(hexValue, 16);
        }
        catch (NumberFormatException e)
        {
            logger.error("Failed to convert binary data to integer ("
                    + hexValue + ")", e);
            return 0;
        }
        return value;
    }

    private void parseCol(boolean generateColnums, boolean generateNull)
            throws OracleExtractException
    {
        int colno;
        long len;
        String data;
        String dateValue;

        if (generateColnums || generateNull)
        {
            colno = parseLastColumnNumber + 1;
        }
        else
        {
            colno = hexVal4();
        }

        if (generateNull)
        {
            len = 0;
            data = "";
        }
        else
        {
            len = hexVal8();
            data = parseData();
        }

        if (logger.isDebugEnabled())
        {
            logger.debug(" - colno = " + colno);
            logger.debug(" - len = " + len);
            logger.debug(" - data = \"" + data + "\"");
        }
        parseLastColumnNumber = colno;
        parseLastColumnData = data;
        parseLastColumn = parseLastTable.findColumn(colno);

        if (generateNull)
        {
            parseLastColumn.setValueNull();
            return;
        }

        if (parseLastColumnData == null || parseLastColumnData.length() == 0)
        {
            parseLastColumn.setValueNull();
        } else {
            switch (parseLastColumn.getType())
            {
                case java.sql.Types.INTEGER :
                case java.sql.Types.NUMERIC :
                    String numberValue = parseOracleNumberToString(parseLastColumnData);
                    if (numberValue.equals(NULL_VALUE))
                    {
                        parseLastColumn.setValueNull();
                    }
                    else
                    {
                        parseLastColumn.setValue(new BigDecimal(numberValue));
                    }
                    break;
                case java.sql.Types.DOUBLE :
                    parseLastColumn
                        .setValue(toDouble(parseToBytes(parseLastColumnData)));
                    break;
                case java.sql.Types.FLOAT :
                    parseLastColumn
                        .setValue(toFloat(parseToBytes(parseLastColumnData)));
                    break;
                case java.sql.Types.VARCHAR :
                    if (parseHexMode) parseLastColumn.setValue(parseToString(parseLastColumnData));
                    else parseLastColumn.setValue(parseLastColumnData);
                    break;
                case java.sql.Types.CHAR :
                    if (parseHexMode) parseLastColumn.setValue(parseToString(parseLastColumnData));
                    else parseLastColumn.setValue(parseLastColumnData);
                    break;
                case java.sql.Types.BLOB :
                    if (parseHexMode) parseLastColumn.setValue(parseToBytes(parseLastColumnData));
                    else parseLastColumn.setValue(parseLastColumnData.getBytes());
                    break;
                case java.sql.Types.CLOB :
                    if (parseHexMode) parseLastColumn.setValue(parseToString(parseLastColumnData));
                    else parseLastColumn.setValue(parseLastColumnData);
                    break;
                case AdditionalTypes.XML :
                    if (parseHexMode) parseLastColumn.setValue(parseToStringUTF16(parseLastColumnData));
                    else parseLastColumn.setValue(parseLastColumnData);
                    break;
                case java.sql.Types.DATE :
                    /*
                     * Other possible Types here include TIME TIMESTAMP
                     */
                    dateValue = parseOracleDateToString(parseLastColumnData);
                    if (dateValue.equals(NULL_VALUE))
                    {
                        parseLastColumn.setValueNull();
                    }
                    else
                    {
                        parseLastColumn.setValue(parseLastDate);
                    }
                    break;
                case java.sql.Types.TIMESTAMP :
                    /*
                     * It appears that the first 7 bytes of an Oracle TIMESTAMP
                     * column are simply stored as a regular Oracle date. The
                     * remaining bytes, if any, represent the fractional portion of
                     * the timestamp.
                     */
                    dateValue = parseOracleDateToString(parseLastColumnData
                            .substring(0, 14));
                    if (dateValue.equals(NULL_VALUE))
                    {
                        parseLastColumn.setValueNull();
                    }
                    else
                    {
                        if (parseLastColumnData.length() > 14)
                        {
                            // There is a fractional part. Extract and process it.
                            String fractionalSeconds = parseLastColumnData
                                    .substring(14);
                            if (fractionalSeconds.length() > 0)
                            {
                                int fractionalPart = convertHexToInt(fractionalSeconds);
                                ((Timestamp) parseLastDate)
                                        .setNanos(fractionalPart);
                            }
                        }
                        parseLastColumn.setValue(parseLastDate);
                    }
                    break;
/*
                case java.sql.Types.BLOB :
                    throw new OracleExtractException("Unsupported Java type BLOB");
                case java.sql.Types.CLOB :
                    throw new OracleExtractException("Unsupported Java type CLOB");
*/
                default :
                    throw new OracleExtractException("Unsupported Java type "
                            + parseLastColumn.getType());
            }
        }
    }

    /*
     * Parse a list of columns String s is expected to be of this format...
     * <ncols><coln0><len0><val0><coln1><len1><val1>....<colnn><lenn><valn>
     * e.g. 0002 0001 8002 c102 0003 000c Scott Martin
     */
    /**
     * parseColumnList definition.
     * 
     * @param orc
     * @param generateColnums
     * @param isKey
     * @param totalColumns - if non-zero generate addition NULL columns until
     *            total is reached.
     */
    private void parseColumnList(OneRowChange orc,
            RowChangeData.ActionType type, boolean isKey, int totalColumns)
            throws OracleExtractException
    {
        int targetColumns = 0;
        int ncols;
        int columnsLeftInRowAfterUpdate = 0;
        boolean generateColnums = (type == RowChangeData.ActionType.INSERT);
        boolean generatingNullsForUpdate = false;

        if (type == RowChangeData.ActionType.UPDATE && !isKey)
        {
            columnsLeftInRowAfterUpdate = hexVal4();
            generatingNullsForUpdate = true;
        }

        ncols = hexVal4();

        if (generatingNullsForUpdate)
        {
            targetColumns = ncols + totalColumns - columnsLeftInRowAfterUpdate;
        }
        else
        {
            if (totalColumns != 0)
                targetColumns = totalColumns;
            else
                targetColumns = ncols;
        }

        ArrayList<ArrayList<OneRowChange.ColumnVal>> rows = (isKey) ? orc
                .getKeyValues() : orc.getColumnValues();

        if (logger.isDebugEnabled())
        {
            logger.debug(" - ncols = " + ncols);
        }

        rows.add(new ArrayList<ColumnVal>());

        parseLastColumnNumber = 0;

        for (int i = 0; i < targetColumns; i++)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug(" - col = " + i);
            }

            ColumnSpec spec = orc.new ColumnSpec();

            if (generatingNullsForUpdate && i >= ncols)
            {
                if (i == ncols) parseLastColumnNumber = columnsLeftInRowAfterUpdate;
                else generateColnums = true;
            }

            parseCol(generateColnums, (i >= ncols));

            spec.setIndex(parseLastColumnNumber);
            spec.setName(parseLastColumn.getName());
            spec.setType(parseLastColumn.getType());
            spec.setNotNull(parseLastColumn.isNotNull());

            ColumnVal value = orc.new ColumnVal();
            value.setValue(parseLastColumn.getValue());

            if (isKey)
            {
                // discard XML type in WHERE clause.
                if (spec.getType() == AdditionalTypes.XML) continue;
                orc.getKeySpec().add(spec);
                orc.getKeyValues().get(0).add(value);
            }
            else
            {
                orc.getColumnSpec().add(spec);
                orc.getColumnValues().get(0).add(value);
            }
        }
    }

    private void initParse(String s)
    {
        parseString = s;
        parseLocation = 0;
    }

    private void parseDML(OneRowChange orc, RowChangeData.ActionType type,
            boolean isKey, String s, int totalColumns)
            throws OracleExtractException
    {
        if (logger.isDebugEnabled())
        {
            logger.debug("- " + (isKey ? "keys" : "data") + " = \"" + s + "\"");
        }
        if (s.length() == 0)
            return;

        initParse(s);

        if (type != RowChangeData.ActionType.INSERT && isKey)
        {
            //int keyLen = hexVal4();
            long keyLen = hexVal8();
            if (keyLen == 0)
                return;
        }

        parseColumnList(orc, type, isKey, totalColumns);
    }

    public OneRowChange parse(String s, String lastSCN)
            throws OracleExtractException
    {
        RowChangeData.ActionType aType;
        OneRowChange orc;
        Boolean performanceTesting = false;
        Table t;

        if (logger.isDebugEnabled())
        {
            logger.debug("Parsing \"" + s + "\"");
        }


        initParse(s);
        String txid = parseColon();
        String rba = parseColon();
        String op = parseColon();
        String obj = parseColon();
        String rid = parseColon();
        String firstCols = parseColon();
        String secondCols = parseColon();

        if (logger.isDebugEnabled())
        {
            logger.debug("- txid = \"" + txid + "\"\n");
            logger.debug("- rba = \"" + rba + "\"\n");
            logger.debug("- op = \"" + op + "\"\n");
        }
        if (op.charAt(0) == 'C')
            return null;

        if (logger.isDebugEnabled())
        {
            logger.debug("- obj = \"" + obj + "\"\n");
            logger.debug("- rid = \"" + rid + "\"\n");
            logger.debug("- 1st = \"" + firstCols + "\"\n");
            logger.debug("- 2nd = \"" + secondCols + "\"\n");
        }
        int objectNumber = java.lang.Integer.parseInt(obj);
        if (performanceTesting) t = findTable(objectNumber, null);
        else t = findTable(objectNumber, lastSCN);

        if (logger.isDebugEnabled())
        {
            if (t != null)
                logger.debug("Found table = " + t.getSchema() + "."
                        + t.getName());
            else
                logger.debug("Cannot find table #" + objectNumber);
        }
        parseLastTable = t;

        /*
         * rowEvent = rowChanges.new OneRowChange();
         * rowEvent.setSchemaName(map.getDbnam());
         * rowEvent.setTableName(map.getTblnam());
         * rowEvent.setAction(RowChangeData.ActionType.UPDATE);
         * rowEvent.setKey(new ArrayList<RowChangeData.OneRowChange.ColumnValue>());
         * rowEvent.setColumns(new ArrayList<RowChangeData.OneRowChange.ColumnValue>());
         */

        switch (op.charAt(0))
        {
            case 'I' :
                aType = RowChangeData.ActionType.INSERT;
                break;
            case 'U' :
                aType = RowChangeData.ActionType.UPDATE;
                break;
            case 'D' :
                aType = RowChangeData.ActionType.DELETE;
                break;
            default :
                throw new OracleExtractException("Bad operation type " + op);

        }

        orc = new OneRowChange(parseLastTable.getSchema(),
                parseLastTable.getName(), aType);

        if (performanceTesting)
        {
            return orc;
        }


        switch (aType)
        {
            case INSERT :
                if (logger.isDebugEnabled())
                {
                    logger.debug("Parsing INSERT");
                }
                parseDML(orc, aType, false, secondCols, t.getColumnCount());
                break;
            case UPDATE :
                if (logger.isDebugEnabled())
                {
                    logger.debug("Parsing UPDATE");
                }
                parseDML(orc, aType, true, firstCols, 0);
                parseDML(orc, aType, false, secondCols, t.getColumnCount());
                break;
            case DELETE :
                if (logger.isDebugEnabled())
                {
                    logger.debug("Parsing DELETE");
                }
                parseDML(orc, aType, true, firstCols, 0);
                break;
            default :
                // TODO What to do in default case ?
                break;
        }

        return orc;
    }
}
