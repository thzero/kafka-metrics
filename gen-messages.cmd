@echo off
:: gen-messages.cmd — Generate Kafka test messages with default or custom settings.
::
:: Usage:
::   gen-messages.cmd                          generates 1000 messages (default distribution)
::   gen-messages.cmd 500                      generates 500 messages (default distribution)
::   gen-messages.cmd 200 10 60 10 20 15       generates 200 messages with custom percentages
::
:: Arguments (all optional, positional):
::   %1  count   — number of messages          (default: 1000)
::   %2  pctNC   — %% New Business              (default: 30)
::   %3  pctEND  — %% Endorsement               (default: 45)
::   %4  pctTRM  — %% Termination               (default:  5)
::   %5  pctRNW  — %% Renewal                   (default: 20)
::   %6  pctBDE  — %% of END that are BDE        (default: 20)
::
:: pctNC + pctEND + pctTRM + pctRNW must equal 100.
:: Output lands in build\generated-messages\ by default.

setlocal

set COUNT=%~1
set PCT_NC=%~2
set PCT_END=%~3
set PCT_TRM=%~4
set PCT_RNW=%~5
set PCT_BDE=%~6

:: Build -P flags for any argument that was provided
set ARGS=
if not "%COUNT%"=="" set ARGS=%ARGS% -Pcount=%COUNT%
if not "%PCT_NC%"=="" set ARGS=%ARGS% -PpctNC=%PCT_NC%
if not "%PCT_END%"=="" set ARGS=%ARGS% -PpctEND=%PCT_END%
if not "%PCT_TRM%"=="" set ARGS=%ARGS% -PpctTRM=%PCT_TRM%
if not "%PCT_RNW%"=="" set ARGS=%ARGS% -PpctRNW=%PCT_RNW%
if not "%PCT_BDE%"=="" set ARGS=%ARGS% -PpctBDE=%PCT_BDE%

echo Running: gradlew.bat generateMessages%ARGS%
call "%~dp0gradlew.bat" generateMessages%ARGS%
