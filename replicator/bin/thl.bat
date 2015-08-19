@echo off
rem VMware Continuent Tungsten Replicator
rem Copyright (C) 2015 VMware, Inc. All rights reserved.
rem
rem Licensed under the Apache License, Version 2.0 (the "License");
rem you may not use this file except in compliance with the License.
rem You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem
rem if "%OS%" == "Windows_NT" setlocal
rem
rem Tungsten Replicator @VERSION@
rem (c) 2009 Continuent, Inc.  All rights reserved. 
rem
rem Replicator Windows control script
rem
rem Environmental variables required by this script: 
rem   REPLICATOR_HOME - Replicator release directory
rem   JAVA_HOME - Java release directory
rem
rem Additional environmental variables accepted by this script.  
rem   JVM_OPTIONS - Java VM options (e.g. -Xmx=1024M)
rem   REPLICATOR_LOG_DIR - Replicator log directory 
rem   REPLICATOR_RMI_PORT - Replicator RMI port
rem   REPLICATOR_CONF_DIR - Location of replicator conf directory
rem

set REPLICATOR_HOME=%~dp0\..

rem Replicator manager class.
set RP_MGR_NAME=com.continuent.tungsten.replicator.thl.THLManagerCtrl

rem
rem Validate REPLICATOR_HOME. 
rem
if not "%REPLICATOR_HOME%" == "" goto REPLICATOR_HOME_DEFINED
echo REPLICATOR_HOME environmental variable must be defined
goto EXIT
:REPLICATOR_HOME_DEFINED
if exist "%REPLICATOR_HOME%\bin\trepctl.bat" goto REPLICATOR_HOME_OK
echo The REPLICATOR_HOME environment variable does not point to a valid release
goto EXIT
:REPLICATOR_HOME_OK

set CLUSTER_HOME=%REPLICATOR_HOME%\..\cluster-home

rem
rem Validate JAVA_HOME. 
rem
if not "%JAVA_HOME%" == "" goto JAVA_HOME_DEFINED
echo JAVA_HOME environmental variable must be defined
goto EXIT
:JAVA_HOME_DEFINED
if exist "%JAVA_HOME%\bin\java.exe" goto JAVA_HOME_OK
echo The JAVA_HOME environment variable does not point to a valid Java release
goto EXIT
:JAVA_HOME_OK

rem
rem Set CLASSPATH. 
rem
set CLASSPATH=%REPLICATOR_HOME%\conf
set CLASSPATH=%CLASSPATH%;%REPLICATOR_HOME%\lib\*
set CLASSPATH=%CLASSPATH%;%REPLICATOR_HOME%\lib-ext\*
set CLASSPATH=%CLASSPATH%;%CLUSTER_HOME%\lib\*

rem 
rem Set log directory location. 
rem
if not "%REPLICATOR_LOG_DIR%" == "" goto REPLICATOR_LOG_DIR_DEFINED
set REPLICATOR_LOG_DIR=%REPLICATOR_HOME%\log
:REPLICATOR_LOG_DIR_DEFINED
set JVM_OPTIONS=%JVM_OPTIONS% -Dreplicator.log.dir="%REPLICATOR_LOG_DIR%"

rem 
rem Set RMI port number. 
rem
if "%REPLICATOR_RMI_PORT%" == "" goto REPLICATOR_RMI_PORT_OK
set JVM_OPTIONS=%JVM_OPTIONS% -Dreplicator.rmi_port=%REPLICATOR_RMI_PORT%
:REPLICATOR_RMI_PORT_OK

rem 
rem Set replicator properties file location. 
rem
if "%REPLICATOR_PROPERTIES%" == "" goto REPLICATOR_PROPERTIES_OK
set JVM_OPTIONS=%JVM_OPTIONS% -Dreplicator.conf.dir="%REPLICATOR_CONF_DIR%"
:REPLICATOR_PROPERTIES_OK

rem Log4j properties to use for THL utility.
set THL_CTRL_LOG4J=log4j-utils.properties

rem Uncomment to debug replicator VM.
rem set REPLICATOR_JVMDEBUG_PORT=54002
rem set JVM_OPTIONS=%JVM_OPTIONS% -enableassertions -Xdebug -Xnoagent -Djava.compiler=none -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=%REPLICATOR_JVMDEBUG_PORT%

rem
rem Start Java VM. 
rem
"%JAVA_HOME%\bin\java" -classpath %CLASSPATH% -Dreplicator.home.dir="%REPLICATOR_HOME%" -Dlog4j.configuration=%TREP_CTRL_LOG4J% %JVM_OPTIONS% %RP_MGR_NAME% %1 %2 %3 %4 %5 %6 %7 %8 %9

:EXIT
if "%OS%" == "Windows_NT" endlocal
