@echo off
setlocal

rem Copyright (c) 1999, 2009 Tanuki Software, Ltd.
rem http://www.tanukisoftware.com
rem All rights reserved.
rem
rem This software is the proprietary information of Tanuki Software.
rem You shall use it only in accordance with the terms of the
rem license agreement you entered into with Tanuki Software.
rem http://wrapper.tanukisoftware.org/doc/english/licenseOverview.html
rem
rem Java Service Wrapper general startup script.
rem Optimized for use with version 3.3.3 of the Wrapper.
rem

set REPLICATOR_HOME=%~dp0\..

rem
rem Resolve the real path of the wrapper.exe
rem  For non NT systems, the _REALPATH and _WRAPPER_CONF values
rem  can be hard-coded below and the following test removed.
rem
if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
rem
rem Find the application home.
rem
rem %~dp0 is location of current script under NT
set _REALPATH=%~dp0

rem
rem Get command line arguments and populate wrapper arguments
rem
set WRAPPER_ARGUMENTS=%1 %2

rem ******************************* wrapper related commands ******************
IF "%1"=="--help" (
echo 
  set WRAPPER_ARGUMENTS=--help
  goto wrapper
)

IF "%1"=="start" (
  set INSTALL_REMOVE_ARG=--install
  set WRAPPER_ARGUMENTS=--start
  goto wrapper
) 
IF "%1"=="stop" (
  set WRAPPER_ARGUMENTS=--remove
  goto wrapper
)
IF "%1"=="status" (
  set WRAPPER_ARGUMENTS=--query
  goto wrapper
)
goto wrapper

:wrapper
rem ---------------------------------------------------------------------------
rem Wrapper command
rem ---------------------------------------------------------------------------
rem Decide on the wrapper binary.
set _WRAPPER_BASE=..\..\cluster-home\bin\wrapper
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%.exe
if exist "%_WRAPPER_EXE%" goto conf
echo Unable to locate a Wrapper executable using any of the following names:
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
echo %_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
echo %_REALPATH%%_WRAPPER_BASE%.exe
pause
goto :eof

rem
rem Find the wrapper.conf
rem
:conf
set _WRAPPER_CONF="%_REALPATH%..\conf\wrapper.conf"

rem
rem Start the Wrapper
rem
if not "%INSTALL_REMOVE_ARG%" == "" (
	"%_WRAPPER_EXE%" %INSTALL_REMOVE_ARG% %_WRAPPER_CONF%
)
:startup
"%_WRAPPER_EXE%" %WRAPPER_ARGUMENTS% %_WRAPPER_CONF%
if not errorlevel 1 goto :eof
pause

