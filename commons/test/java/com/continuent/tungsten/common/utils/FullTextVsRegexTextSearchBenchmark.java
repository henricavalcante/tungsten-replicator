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
 * Initial developer(s): Gilles Rayrat
 * Contributor(s): 
 */

package com.continuent.tungsten.common.utils;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.regex.Pattern;

import junit.framework.TestCase;

/**
 * Benchmark of text search to compare full text and regular expression search.
 * This is not a proper unit test, it just displays results
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public class FullTextVsRegexTextSearchBenchmark extends TestCase
{
    /**
     * Test are run 10 times. This puts in evidence weird runs such as where
     * some other tasks are eating CPU cycles
     */
    private static final int   NUMBER_OF_RUNS                      = 5;
    /**
     * This is the number of times the search should be run. 1 million simple
     * searches take a few seconds
     */
    private static final int   NUMBER_OF_ITERATIONS                = 1000000;
    public static final String SELECT                              = "SELECT";
    public static final String SELECT_REGEX                        = "^SELECT";
    public static final String SELECT_CASE_INSENSITIVE_GM_TRICK    = "^[sS][eE][lL][eE][cC][tT]";
    public static Pattern      SELECT_PATTERN_INSENSITIVE          = Pattern
                                                                           .compile(
                                                                                   SELECT_REGEX,
                                                                                   Pattern.CASE_INSENSITIVE);
    public static Pattern      SELECT_PATTERN_SENSITIVE            = Pattern
                                                                           .compile(SELECT_REGEX);
    public static Pattern      SELECT_PATTERN_INSENSITIVE_GM_TRICK = Pattern
                                                                           .compile(SELECT_CASE_INSENSITIVE_GM_TRICK);
    /** used to generate random text */
    private SecureRandom       random                              = new SecureRandom();

    public void testTextSearch()
    {
        // Simple request
        System.out.println("************ " + NUMBER_OF_RUNS
                + " runs with SIMPLE SELECT ***************");
        for (int i = 0; i < NUMBER_OF_RUNS; i++)
        {
            runTextSearchTestWith(SELECT + " * FROM mytable");
        }
        // With a random string of ~200 characters. We keep the select keyword
        // so that the string is always matched
        System.out.println("************ " + NUMBER_OF_RUNS
                + " runs with MEDIUM STRING ***************");
        for (int i = 0; i < NUMBER_OF_RUNS; i++)
        {
            String s = new BigInteger(1024, random).toString(32);
            runTextSearchTestWith(SELECT + s);
        }
        // With a random string of ~2000 characters. We keep the select keyword
        // so that the string is always matched
        System.out.println("************ " + NUMBER_OF_RUNS
                + " runs with LARGE STRING ***************");
        for (int i = 0; i < NUMBER_OF_RUNS; i++)
        {
            String s = new BigInteger(1024 * 10, random).toString(32);
            runTextSearchTestWith(SELECT + s);
        }
    }

    public void runTextSearchTestWith(String sql)
    {
        // comment / uncomment the tests you want to run here:

        long before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestFullTextCaseSensitive(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for full text case insensitive search");

        // before = System.currentTimeMillis();
        // for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        // {
        // analyzeRequestFullTextWithToUpper(sql);
        // }
        // System.out.println(System.currentTimeMillis() - before
        // + "\t ms for full text case sensitive/toUpper search");

        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestFullTextWithToUpperOfRelevantPart(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for full text case sensitive/toUpper search");

        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestFullTextWithTrim(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for full text with trim search");

        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestFullTextWithRegionMatches(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for full text with regionMatches search");

        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestFullTextCaseInsensitive(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for full text case sensitive/double compare search");
        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestFullTextWithAFewPatterns(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for full text case sensitive/a few compare search");
        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestFullTextWithLotsOfPatterns(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for full text case sensitive/a lot compare search");

        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestRegexCaseSensitive(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for regex case sensitive search");

        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestRegexCaseInsensitive(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for regex case insensitive search");

        before = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_ITERATIONS; i++)
        {
            analyzeRequestRegexCaseInsensitiveGMtrick(sql);
        }
        System.out.println(System.currentTimeMillis() - before
                + "\t ms for regex case insensitive/GM trick search");

        System.out.println();
    }

    public boolean analyzeRequestFullTextCaseSensitive(String request)
    {
        if (request.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextWithToUpper(String request)
    {
        String requestUpperCase = request.toUpperCase();
        if (requestUpperCase.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextWithToUpperOf15Chars(String request)
    {
        String requestPrefixUpperCase = request.substring(0,
                Math.min(request.length(), 15)).toUpperCase();
        if (requestPrefixUpperCase.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextWithToUpperOfRelevantPart(
            String request)
    {
        String requestPrefixUpperCase = request.substring(0,
                Math.min(request.length(), SELECT.length())).toUpperCase();
        if (requestPrefixUpperCase.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextWithTrim(String request)
    {
        String requestUpperCase = request.trim();
        if (requestUpperCase.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextWithRegionMatches(String request)
    {
        String requestUpperCase = request.trim();
        if (requestUpperCase.regionMatches(true, 0, SELECT, 0, SELECT.length()))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextCaseInsensitive(String request)
    {
        if (request.startsWith(SELECT) || request.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextWithAFewPatterns(String request)
    {
        if (request.startsWith("APDFIA") || request.startsWith("EFNEPW")
                || request.startsWith("VFSPUC") || request.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestFullTextWithLotsOfPatterns(String request)
    {
        if (request.startsWith("select") || request.startsWith("Select")
                || request.startsWith("sElect") || request.startsWith("seLect")
                || request.startsWith("selEct") || request.startsWith("seleCt")
                || request.startsWith("selecT") || request.startsWith("SElect")
                || request.startsWith("SeLect") || request.startsWith("SelEct")
                || request.startsWith("SeleCt") || request.startsWith("SelecT")
                || request.startsWith("SElect") || request.startsWith("SElEct")
                || request.startsWith("SEleCt") || request.startsWith("SElecT")
                || request.startsWith("SELect") || request.startsWith("SELeCt")
                || request.startsWith("SELecT") || request.startsWith("SELEct")
                || request.startsWith("SELEcT") || request.startsWith("SELECt")
                || request.startsWith("SeLECT") || request.startsWith("SElECT")
                || request.startsWith("SELeCT") || request.startsWith("SELEcT")
                || request.startsWith("SELECt") || request.startsWith("SelECT")
                || request.startsWith("SeLeCT") || request.startsWith("SeLEcT")
                || request.startsWith("SeLECt") || request.startsWith("SelECT")
                || request.startsWith("SelEcT") || request.startsWith("SelECt")
                || request.startsWith("SeleCT") || request.startsWith("SeleCt")
                || request.startsWith("SelecT") || request.startsWith("Select")
                || request.startsWith(SELECT))
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestRegexCaseSensitive(String request)
    {
        if (SELECT_PATTERN_SENSITIVE.matcher(request).find())
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestRegexCaseInsensitive(String request)
    {
        if (SELECT_PATTERN_INSENSITIVE.matcher(request).find())
        {
            return true;
        }
        return false;
    }

    public boolean analyzeRequestRegexCaseInsensitiveGMtrick(String request)
    {
        if (SELECT_PATTERN_INSENSITIVE_GM_TRICK.matcher(request).find())
        {
            return true;
        }
        return false;
    }
}
