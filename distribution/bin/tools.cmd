@echo off
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.

if not exist "%JAVA_HOME%\bin\java.exe" echo Please set the JAVA_HOME variable in your environment, We need java(x64)! & EXIT /B 1

set "JAVA=%JAVA_HOME%\bin\java.exe"

setlocal
set BASE_DIR=%~dp0
set BASE_DIR=%BASE_DIR:~0,-1%
for %%d in (%BASE_DIR%) do set BASE_DIR=%%~dpd

set CLASSPATH=.;%BASE_DIR%conf;%CLASSPATH%

for /f tokens^=2-5^ delims^=.-_^" %%j in ('java -fullversion 2^>^&1') do @set "JAVA_VERSION=%%j"
if %JAVA_VERSION% EQU 1 goto java8-
goto java9+

rem ===========================================================================================
rem JVM Configuration
rem ===========================================================================================

:java8-
set "JAVA_OPT=%JAVA_OPT% -server -Xms1g -Xmx1g -Xmn256m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=128m"
set "JAVA_OPT=%JAVA_OPT% -Djava.ext.dirs="%BASE_DIR%\lib";"%JAVA_HOME%\jre\lib\ext";"%JAVA_HOME%\lib\ext""
set "JAVA_OPT=%JAVA_OPT% -cp "%CLASSPATH%""
goto end
:java9+
set "JAVA_OPT=%JAVA_OPT% --add-exports java.base/jdk.internal.ref=ALL-UNNAMED -server -Xms1g -Xmx1g -Xmn256m -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=128m"
set "JAVA_OPT=%JAVA_OPT% --class-path="%CLASSPATH%";"%BASE_DIR%\lib\*";"%JAVA_HOME%\jre\lib\ext";"%JAVA_HOME%\lib\ext""
:end

"%JAVA%" %JAVA_OPT% -version
