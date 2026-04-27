@echo off
:: gen-messages.cmd — Generate IIF metrics Kafka test messages.
::
:: Usage:
::   gen-messages.cmd               generates 1000 messages (default)
::   gen-messages.cmd 500           generates 500 messages
::
:: Arguments (all optional):
::   %1  count   — number of messages to generate (default: 1000)
::
:: Agreement product numbers and product combos are derived from the seeded
:: reference data (IifDataSeeder Random(42)) so all messages will process cleanly.
:: Output lands in build\generated-messages\messages-<count>.jsonl

setlocal

set COUNT=%~1

set ARGS=
if not "%COUNT%"=="" set ARGS=%ARGS% -Pcount=%COUNT%

echo Running: gradlew.bat generateMessages%ARGS%
call "%~dp0gradlew.bat" generateMessages%ARGS%
