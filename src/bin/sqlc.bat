@echo off & setlocal enabledelayedexpansion

set curdir=%~dp0
set params=

:param
set str=%1
if "%str%"=="" (
    goto end
)
set params=%params% %str%
shift /0
goto param

:end
if "%params%"=="" (
    goto eof
)

:intercept_left
if "%params:~0,1%"==" " set "params=%params:~1%"&goto intercept_left
:intercept_right
if "%params:~-1%"==" " set "params=%params:~0,-1%"&goto intercept_right

:eof
java -jar %curdir%\\lib\\${name}-${version}.jar %params%

pause