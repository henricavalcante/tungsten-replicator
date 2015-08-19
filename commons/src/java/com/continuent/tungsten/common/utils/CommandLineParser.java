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
 * Initial developer(s): Ed Archibald
 * Contributor(s): Linas Virbalas, Gilles Rayrat
 */

package com.continuent.tungsten.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Vector;

import jline.ConsoleReader;

public class CommandLineParser
{
    private static final String FLAG_INTRODUCER    = "-";
    private static final char   LONG_FLAG          = 'l';
    private static final char   RECURSIVE_FLAG     = 'R';
    public static final char    ABSOLUTE_FLAG      = 'A';
    public static final char    PARENTS_FLAG       = 'p';
    public static final char    BACKGROUND_FLAG    = '&';

    private static final String BACKGROUND_TOKEN   = "&";
    private static final String REDIRECT_OUT_TOKEN = ">";
    private static final String REDIRECT_IN_TOKEN  = "<";

    public static void main(String[] args)
    {
        try
        {
            ConsoleReader reader = new ConsoleReader();
            CommandLineParser parser = new CommandLineParser();
            Vector<Command> cmds = null;

            while ((cmds = parser.getCommand(reader, "> ", null, true)) != null)
            {
                for (Command cmd : cmds)
                {
                    System.out.println(CLUtils.printArgs(cmd.getTokens()));
                    System.out.println(cmd.isBackground()
                            ? "BACKGROUND"
                            : "FOREGROUND");
                    System.out.println(cmd.isLong() ? "LONG" : "SHORT");
                    System.out
                            .println(cmd.isRecursive() ? "RECURSIVE" : "FLAT");
                    System.out.println(cmd.isRedirectInput() ? "INPUT="
                            + cmd.getInput() : "STDIN");
                    System.out.println(cmd.isRedirectOutput() ? "OUTPUT="
                            + cmd.getOutput() : "STDOUT");
                }

            }
        }
        catch (IOException e)
        {
            System.out.println(e);
        }
    }

    public Command parseOne(String commandLine, boolean parseFlags)
    {
        Vector<Command> cmds = parse(commandLine, parseFlags);

        if (cmds.size() == 1)
        {
            return cmds.get(0);
        }
        else if (cmds.size() > 1)
        {
            CLUtils.println("Multiple commands, separated by ';' are not allowed here.");
        }

        return null;
    }

    public Command parseOne(String commandLine)
    {
        return parseOne(commandLine, true);
    }

    public Vector<Command> parse(String commandLine)
    {
        return parse(commandLine, true);
    }

    /**
     * This method parses a set of tokens returned by a simple command line
     * parser, strips out any 'meta' commands and sets the appropriate flags,
     * and returns the 'clean' set of tokens to be processed by the command
     * processor.
     */
    public Vector<Command> parse(String commandBuf, boolean parseFlags)
    {

        Vector<Command> commandList = new Vector<Command>();

        String[] commands = commandBuf.split(";");

        for (String commandLine : commands)
        {

            Command command = new Command(commandLine);

            String splitPattern = null;

            if (parseFlags)
            {
                splitPattern = "\\s+|\\s+-\\b|'|\"";
            }
            else
            {
                splitPattern = "\\s+|'|\"";
            }

            // First strip blanks
            Vector<String> noBlanks = new Vector<String>();
            noBlanks.toArray(new String[noBlanks.size()]);
            String[] tokens = commandLine.split(splitPattern);
            for (String token : tokens)
            {
                if (!token.trim().equals(""))
                    noBlanks.add(token.trim());
            }

            // If there are no non-blank tokens, just return null;
            if (noBlanks.size() == 0)
            {
                return null;
            }

            // Now evaluate the tokens and set appropriate flags,
            // stripping out any tokens that are flag-specific
            int i = 0;
            while (true)
            {
                if (i == noBlanks.size())
                    break;

                String currentToken = noBlanks.get(i);

                if (parseFlags
                        && currentToken.trim().startsWith(FLAG_INTRODUCER))
                {
                    char[] flagChars = currentToken.trim().toCharArray();

                    for (int j = 1; j < flagChars.length; j++)
                    {
                        char flagChar = flagChars[j];

                        if (flagChar == LONG_FLAG)
                            command.setIsLong(true);
                        else if (flagChar == RECURSIVE_FLAG)
                            command.setIsRecursive(true);
                        else if (flagChar == PARENTS_FLAG)
                            command.setIncludeParents(true);
                        else if (flagChar == ABSOLUTE_FLAG)
                            command.setIsAbsolute(true);
                        else if (flagChar == BACKGROUND_FLAG)
                        {
                            if (i + 1 == noBlanks.size()
                                    && (j + 1 == flagChars.length))
                            {
                                command.setIsBackground(true);
                            }
                            else
                                CLUtils.println("The token '&' can only appear at the end of a command");
                        }
                    }
                }
                else if (currentToken.trim().equals(REDIRECT_IN_TOKEN))
                {
                    if (i + 1 < noBlanks.size())
                    {
                        String input = noBlanks.get(++i);
                        command.setIsRedirectInput(true, input);
                    }
                    else
                    {
                        CLUtils.println("No arg supplied for input redirection");
                    }
                }
                else if (currentToken.trim().equals(REDIRECT_OUT_TOKEN))
                {
                    if (i + 1 < noBlanks.size())
                    {
                        String output = noBlanks.get(++i);
                        command.setIsRedirectOutput(true, output);
                    }
                    else
                    {
                        CLUtils.println("No arg supplied for output redirection");
                    }
                }
                else if (currentToken.trim().endsWith(BACKGROUND_TOKEN))
                {
                    if (i + 1 == noBlanks.size())
                    {
                        command.setIsBackground(true);
                        if (currentToken.trim().length() > 1)
                        {
                            currentToken = currentToken.trim().substring(0,
                                    currentToken.indexOf(BACKGROUND_TOKEN));
                            command.addToken(currentToken);
                        }
                    }
                    else
                        CLUtils.println("The token '&' can only appear at the end of a command");
                }
                else
                {
                    command.addToken(currentToken);
                }

                i++;
            }

            if (command.getTokens() != null)
            {
                commandList.add(command);
            }
        }

        return commandList;
    }

    public Vector<Command> getCommand(ConsoleReader cr, String prompt,
            BufferedReader in) throws IOException
    {
        return getCommand(cr, prompt, in, true);
    }

    public Vector<Command> getCommand(ConsoleReader cr, String prompt,
            BufferedReader in, boolean printPrompt) throws IOException
    {
        String inbuf = null;

        if (cr != null)
        {
            if (printPrompt)
                inbuf = cr.readLine(prompt);
            else
                inbuf = cr.readLine("");
        }
        else
        {
            if (printPrompt)
                System.out.print(prompt);
            CLUtils.println("Using in.readline()");
            inbuf = in.readLine();
        }
        if (inbuf == null)
        {
            CLUtils.println("\nExiting...");
            System.exit(0);
        }

        return parse(inbuf, true);
    }

}
