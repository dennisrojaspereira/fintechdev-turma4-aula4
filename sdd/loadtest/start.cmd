@echo off
rem Starts the classroom runner (which brings the docker compose stack up from the page) and
rem opens the page in the default browser. Requires JDK 21 and Docker Desktop running.
cd /d "%~dp0"
start "" http://localhost:7000/
java K6Runner.java %*
