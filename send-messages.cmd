@echo off
:: send-messages.cmd — Sends a generated JSONL file to the Kafka input topic.
::
:: Usage:
::   send-messages.cmd                                 sends build\generated-messages\most recent messages-*.jsonl
::   send-messages.cmd build\generated-messages\messages-100.jsonl
::   send-messages.cmd path\to\messages.jsonl input-topic
::
:: Arguments (all optional):
::   %1  path to JSONL file  (default: most recently modified messages-*.jsonl in build\generated-messages\)
::   %2  Kafka topic          (default: input-topic)
::
:: Requires: Docker running with the kafka container from docker-compose.yml

setlocal enabledelayedexpansion

set TOPIC=%~2
if "%TOPIC%"=="" set TOPIC=input-topic

set JSONL_FILE=%~1
if "%JSONL_FILE%"=="" (
    :: Find the most recently modified messages-*.jsonl
    set JSONL_FILE=
    for /f "delims=" %%F in ('dir /b /o:-d "build\generated-messages\messages-*.jsonl" 2^>nul') do (
        if "!JSONL_FILE!"=="" set JSONL_FILE=build\generated-messages\%%F
    )
)

if "%JSONL_FILE%"=="" (
    echo ERROR: No JSONL file found. Run gen-messages.cmd first.
    exit /b 1
)

if not exist "%JSONL_FILE%" (
    echo ERROR: File not found: %JSONL_FILE%
    exit /b 1
)

::  Count lines in the file — find /c outputs "---------- FILE: N", grab last token
set COUNT=0
for /f "tokens=*" %%L in ('find /c /v "" "%JSONL_FILE%"') do set FIND_OUT=%%L
for /f "tokens=2 delims=:" %%N in ("!FIND_OUT!") do set COUNT=%%N
set COUNT=%COUNT: =%

echo Sending %COUNT% messages from %JSONL_FILE% to topic [%TOPIC%]...
echo.

:: Copy the file into the kafka container then pipe it through kafka-console-producer
docker cp "%JSONL_FILE%" kafka:/tmp/messages.jsonl
docker exec kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic %TOPIC% < /tmp/messages.jsonl"

if %ERRORLEVEL% equ 0 (
    echo.
    echo Done. %COUNT% messages sent to [%TOPIC%].
    echo Watch them arrive at http://localhost:8081 ^(Kafka UI^)
) else (
    echo ERROR: kafka-console-producer failed ^(exit code %ERRORLEVEL%^).
    echo Is the kafka container running? Try: docker compose up -d
)
