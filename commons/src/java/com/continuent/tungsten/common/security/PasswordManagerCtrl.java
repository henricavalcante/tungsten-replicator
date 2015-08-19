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
 * Initial developer(s): Ludovic Launer
 * Contributor(s): 
 */

package com.continuent.tungsten.common.security;

import java.text.MessageFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.cluster.ConfigurationException;
import com.continuent.tungsten.common.jmx.ServerRuntimeException;
import com.continuent.tungsten.common.security.PasswordManager.ClientApplicationType;

/**
 * Application to manage passwords and users
 * 
 * @author <a href="mailto:ludovic.launer@continuent.com">Ludovic Launer</a>
 * @version 1.0
 */
public class PasswordManagerCtrl
{

    private static Logger                                logger                  = Logger.getLogger(PasswordManagerCtrl.class);

    private Options                                      helpOptions             = new Options();
    private Options                                      options                 = new Options();
    private static PasswordManagerCtrl                   pwd;
    private static PasswordManager.ClientApplicationType clientApplicationType   = null;
    public PasswordManager                               passwordManager         = null;

    // --- Options overriding elements in security.properties ---
    private Boolean                                      useEncryptedPassword    = null;
    private String                                       truststoreLocation      = null;
    private String                                       truststorePassword      = null;
    private String                                       keystoreLocation        = null;
    private String                                       keystorePassword        = null;
    private String                                       passwordFileLocation    = null;

    // -- Define constants for command line arguments ---
    private static final String                          HELP                    = "help";
    private static final String                          _HELP                   = "h";
    private static final String                          _AUTHENTICATE           = "a";
    private static final String                          AUTHENTICATE            = "authenticate";
    private static final String                          CREATE                  = "create";
    private static final String                          _CREATE                 = "c";
    private static final String                          DELETE                  = "delete";
    private static final String                          _DELETE                 = "d";
    private static final String                          FILE                    = "file";
    private static final String                          _FILE                   = "f";
    private static final String                          TARGET_APPLICATION      = "target";
    private static final String                          _TARGET_APPLICATION     = "t";
    private static final String                          _ENCRYPTED_PASSWORD     = "e";
    private static final String                          ENCRYPTED_PASSWORD      = "encrypted.password";
    private static final String                          _TRUSTSTORE_LOCATION    = "ts";
    private static final String                          TRUSTSTORE_LOCATION     = "truststore.location";
    private static final String                          _TRUSTSTORE_PASSWORD    = "tsp";
    private static final String                          TRUSTSTORE_PASSWORD     = "truststore.password";
    private static final String                          KEYSTORE_LOCATION       = "keystore.location";
    private static final String                          _KEYSTORE_LOCATION      = "ks";
    private static final String                          KEYSTORE_PASSWORD       = "keystore.password";
    private static final String                          _KEYSTORE_PASSWORD      = "ksp";
    private static final String                          _PASSWORD_FILE_LOCATION = "p";
    private static final String                          PASSWORD_FILE_LOCATION  = "password_file.location";

    private static Option                                create;
    private static Option                                authenticate;

    // --- Exit codes ---
    private enum EXIT_CODE
    {
        EXIT_OK(0),
        EXIT_ERROR(1);

        private final int value;

        private EXIT_CODE(int value)
        {
            this.value = value;
        }
    }

    /**
     * Setup command line options
     */
    @SuppressWarnings("static-access")
    private void setupCommandLine()
    {
        // --- Options on the command line ---
        Option help = OptionBuilder.withLongOpt(HELP)
                .withDescription("Displays this message").create(_HELP);
        Option file = OptionBuilder
                .withLongOpt(FILE)
                .withArgName("filename")
                .hasArgs()
                .withDescription(
                        "Location of the "
                                + SecurityConf.SECURITY_PROPERTIES_FILE_NAME
                                + " file").create(_FILE);

        // Mutually excluding options
        OptionGroup optionGroup = new OptionGroup();
        authenticate = OptionBuilder.withLongOpt(AUTHENTICATE).hasArgs(2)
                .withArgName("username> <password")
                .withDescription("Authenticates a user with given password")
                .create(_AUTHENTICATE);
        create = OptionBuilder.withLongOpt(CREATE).hasArgs(2)
                .withArgName("username> <password")
                .withDescription("Creates or Updates a user").create(_CREATE);
        Option delete = OptionBuilder.withLongOpt(DELETE)
                .withArgName("username").hasArgs()
                .withDescription("Deletes a user").create(_DELETE);

        optionGroup.addOption(authenticate);
        optionGroup.addOption(create);
        optionGroup.addOption(delete);
        optionGroup.setRequired(true);      // At least 1 command required

        Option targetApplication = OptionBuilder
                .withLongOpt(TARGET_APPLICATION)
                .withArgName("target")
                .hasArgs()
                .withDescription(
                        "Target application: "
                                + getListOfClientApplicationType())
                .create(_TARGET_APPLICATION);

        // --- Options replacing parameters from security.properties ---
        Option encryptedPassword = OptionBuilder
                .withLongOpt(ENCRYPTED_PASSWORD)
                .withArgName("encrypt password")
                .withDescription("Encrypts the password")
                .create(_ENCRYPTED_PASSWORD);
        Option truststoreLocation = OptionBuilder
                .withLongOpt(TRUSTSTORE_LOCATION).withArgName("filename")
                .hasArg()
                .withDescription("Location of the tuststore file")
                .create(_TRUSTSTORE_LOCATION);
        Option truststorePassword = OptionBuilder
                .withLongOpt(TRUSTSTORE_PASSWORD).withArgName("password")
                .hasArg()
                .withDescription("Password for the truststore file")
                .create(_TRUSTSTORE_PASSWORD);
        Option keystoreLocation = OptionBuilder
                .withLongOpt(KEYSTORE_LOCATION).withArgName("filename")
                .hasArg()
                .withDescription("Location of the keystore file")
                .create(_KEYSTORE_LOCATION);
        Option keystorePassword = OptionBuilder
                .withLongOpt(KEYSTORE_PASSWORD).withArgName("password")
                .hasArg()
                .withDescription("Password for the keystore file")
                .create(_KEYSTORE_PASSWORD);
        Option passwordFileLocation = OptionBuilder
                .withLongOpt(PASSWORD_FILE_LOCATION).withArgName("filename")
                .hasArg()
                .withDescription("Location of the password file")
                .create(_PASSWORD_FILE_LOCATION);

        // --- Add options to the list ---
        // --- Help
        this.helpOptions.addOption(help);

        // --- Program command line options
        this.options.addOptionGroup(optionGroup);
        this.options.addOption(file);
        this.options.addOption(help);
        this.options.addOption(encryptedPassword);
        this.options.addOption(truststoreLocation);
        this.options.addOption(truststorePassword);
        this.options.addOption(keystoreLocation);
        this.options.addOption(keystorePassword);
        this.options.addOption(passwordFileLocation);

        this.options.addOption(targetApplication);
    }

    /**
     * Creates a new <code>PasswordManager</code> object
     */
    public PasswordManagerCtrl()
    {
        this.setupCommandLine();
    }

    /**
     * Password Manager entry point
     * 
     * @param argv
     * @throws Exception
     */
    public static void main(String argv[]) throws Exception
    {
        pwd = new PasswordManagerCtrl();

        // --- Options ---
        ClientApplicationType clientApplicationType = null;
        String securityPropertiesFileLocation = null;
        String username = null;
        String password = null;
        CommandLine line = null;

        try
        {
            CommandLineParser parser = new GnuParser();
            // --- Parse the command line arguments ---

            // --- Help
            line = parser.parse(pwd.helpOptions, argv, true);
            if (line.hasOption(_HELP))
            {
                DisplayHelpAndExit(EXIT_CODE.EXIT_OK);
            }

            // --- Program command line options
            line = parser.parse(pwd.options, argv);

            // --- Handle options ---

            // --- Optional arguments : Get options ---
            if (line.hasOption(_HELP))
            {
                DisplayHelpAndExit(EXIT_CODE.EXIT_OK);
            }
            if (line.hasOption(_TARGET_APPLICATION))                        // Target Application
            {
                String target = line.getOptionValue(TARGET_APPLICATION);
                clientApplicationType = PasswordManagerCtrl
                        .getClientApplicationType(target);
            }
            if (line.hasOption(_FILE))                                      // security.properties file location
            {
                securityPropertiesFileLocation = line.getOptionValue(_FILE);
            }
            if (line.hasOption(_AUTHENTICATE))
            {                   // Make sure username + password are provided
                String[] authenticateArgs = line.getOptionValues(_AUTHENTICATE);
                if (authenticateArgs.length < 2)
                    throw new MissingArgumentException(authenticate);

                username = authenticateArgs[0];
                password = authenticateArgs[1];
            }
            if (line.hasOption(_CREATE))
            {                   // Make sure username + password are provided
                String[] createArgs = line.getOptionValues(_CREATE);
                if (createArgs.length < 2)
                    throw new MissingArgumentException(create);

                username = createArgs[0];
                password = createArgs[1];
            }
            // --- Options to replace values in security.properties file ---
            if (line.hasOption(_ENCRYPTED_PASSWORD))
                pwd.useEncryptedPassword = true;
            if (line.hasOption(_TRUSTSTORE_LOCATION))
                pwd.truststoreLocation = line
                        .getOptionValue(_TRUSTSTORE_LOCATION);
            if (line.hasOption(_TRUSTSTORE_PASSWORD))
                pwd.truststorePassword = line
                        .getOptionValue(_TRUSTSTORE_PASSWORD);
            if (line.hasOption(_KEYSTORE_LOCATION))
                pwd.keystoreLocation = line
                        .getOptionValue(_KEYSTORE_LOCATION);
            if (line.hasOption(_KEYSTORE_PASSWORD))
                pwd.keystorePassword = line
                        .getOptionValue(_KEYSTORE_PASSWORD);
            if (line.hasOption(_PASSWORD_FILE_LOCATION))
                pwd.passwordFileLocation = (String) line
                        .getOptionValue(_PASSWORD_FILE_LOCATION);

            try
            {
                pwd.passwordManager = new PasswordManager(
                        securityPropertiesFileLocation, clientApplicationType);

                AuthenticationInfo authenticationInfo = pwd.passwordManager
                        .getAuthenticationInfo();
                // --- Substitute with user provided options
                if (pwd.useEncryptedPassword != null)
                    authenticationInfo
                            .setUseEncryptedPasswords(pwd.useEncryptedPassword);
                if (pwd.truststoreLocation != null)
                    authenticationInfo
                            .setTruststoreLocation(pwd.truststoreLocation);
                if (pwd.truststorePassword != null)
                    authenticationInfo
                            .setTruststorePassword(pwd.truststorePassword);
                if (pwd.keystoreLocation != null)
                    authenticationInfo
                            .setKeystoreLocation(pwd.keystoreLocation);
                if (pwd.keystorePassword != null)
                    authenticationInfo
                            .setKeystorePassword(pwd.keystorePassword);
                if (pwd.passwordFileLocation != null)
                    authenticationInfo
                            .setPasswordFileLocation(pwd.passwordFileLocation);

                // --- Display summary of used parameters ---
                logger.info("Using parameters: ");
                logger.info("-----------------");
                if (authenticationInfo.getParentPropertiesFileLocation() != null)
                    logger.info(MessageFormat.format(
                            "security.properties \t = {0}", authenticationInfo
                                    .getParentPropertiesFileLocation()));
                logger.info(MessageFormat.format(
                        "password_file.location \t = {0}",
                        authenticationInfo.getPasswordFileLocation()));
                logger.info(MessageFormat.format("encrypted.password \t = {0}",
                        authenticationInfo.isUseEncryptedPasswords()));

                // --- Keystore
                if (line.hasOption(_AUTHENTICATE))
                {
                    logger.info(MessageFormat.format(
                            "keystore.location \t = {0}",
                            authenticationInfo.getKeystoreLocation()));
                    logger.info(MessageFormat.format(
                            "keystore.password \t = {0}",
                            authenticationInfo.getKeystorePassword()));
                }

                // --- Truststore
                if (authenticationInfo.isUseEncryptedPasswords())
                {
                    logger.info(MessageFormat.format(
                            "truststore.location \t = {0}",
                            authenticationInfo.getTruststoreLocation()));
                    logger.info(MessageFormat.format(
                            "truststore.password \t = {0}",
                            authenticationInfo.getTruststorePassword()));
                }
                logger.info("-----------------");

                // --- AuthenticationInfo consistency check
                // Try to create files if possible
                pwd.passwordManager.try_createAuthenticationInfoFiles();
                authenticationInfo.checkAndCleanAuthenticationInfo();

            }
            catch (ConfigurationException ce)
            {
                logger.error(MessageFormat
                        .format("Could not retrieve configuration information: {0}\nTry to specify a security.properties file location, provide options on the command line, or have the cluster.home variable set.",
                                ce.getMessage()));
                System.exit(EXIT_CODE.EXIT_ERROR.value);
            }
            catch (ServerRuntimeException sre)
            {
                logger.error(sre.getLocalizedMessage());
                // AuthenticationInfo consistency check : failed
                DisplayHelpAndExit(EXIT_CODE.EXIT_ERROR);
            }

            // --- Perform commands ---

            // ######### Authenticate ##########
            if (line.hasOption(_AUTHENTICATE))
            {
                try
                {
                    boolean authOK = pwd.passwordManager.authenticateUser(
                            username, password);
                    String msgAuthOK = (authOK) ? "SUCCESS" : "FAILED";
                    logger.info(MessageFormat.format(
                            "Authenticating  {0}:{1} = {2}",
                            username, password, msgAuthOK));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format(
                            "Error while authenticating user: {0}",
                            e.getMessage()));
                }
            }
            // ######### Create ##########
            if (line.hasOption(_CREATE))
            {
                try
                {
                    pwd.passwordManager.setPasswordForUser(username, password);
                    logger.info(MessageFormat.format(
                            "User created successfuly: {0}", username));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format(
                            "Error while creating user: {0}", e.getMessage()));
                }
            }

            // ########## DELETE ##########
            else if (line.hasOption(_DELETE))
            {
                username = line.getOptionValue(_DELETE);

                try
                {
                    pwd.passwordManager.deleteUser(username);
                    logger.info(MessageFormat.format(
                            "User deleted successfuly: {0}", username));
                }
                catch (Exception e)
                {
                    logger.error(MessageFormat.format(
                            "Error while deleting user: {0}", e.getMessage()));
                }
            }

        }
        catch (ParseException exp)
        {
            logger.error(exp.getMessage());

            DisplayHelpAndExit(EXIT_CODE.EXIT_ERROR);
        }
    }

    /**
     * Get the list of ClientApplicationType as a string
     * 
     * @return String containing all possible lower-cased application types
     */
    private static String getListOfClientApplicationType()
    {
        String listApplicationType = "";

        for (ClientApplicationType appType : ClientApplicationType.values())
        {
            listApplicationType += ((listApplicationType.isEmpty() ? "" : " | ") + appType
                    .toString().toLowerCase());
        }
        listApplicationType.trim();

        return listApplicationType;
    }

    /**
     * Get the application type argument.
     * Populates the <code>clientApplicationType</code> property
     * 
     * @param commandLineTargetApplicationType the argument provided on the
     *            command line
     * @return a ClientApplicationType
     * @throws UnrecognizedOptionException if the cast could not be made
     */
    private static ClientApplicationType getClientApplicationType(
            String commandLineTargetApplicationType)
            throws UnrecognizedOptionException
    {
        try
        {
            clientApplicationType = ClientApplicationType
                    .fromString(commandLineTargetApplicationType);
        }
        catch (IllegalArgumentException iae)
        {
            throw new UnrecognizedOptionException(MessageFormat.format(
                    "The target application type does not exist: {0}",
                    commandLineTargetApplicationType));
        }

        return clientApplicationType;
    }

    /**
     * Display the program help and exits
     */
    private static void DisplayHelpAndExit(EXIT_CODE exitCode)
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);
        formatter.printHelp("tpasswd", pwd.options);
        Exit(exitCode);
    }

    private static void Exit(EXIT_CODE exitCode)
    {
        System.exit(exitCode.value);
    }

}
