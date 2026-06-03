@echo off
setlocal

set IMAGE_NAME=hhlh1/fastdds-csv-app

if "%~1"=="" (
    set TAG=latest
) else (
    set TAG=%~1
)

echo Building image %IMAGE_NAME%:%TAG% ...
docker build -t %IMAGE_NAME%:%TAG% .
if errorlevel 1 goto :error

echo Pushing image %IMAGE_NAME%:%TAG% ...
docker push %IMAGE_NAME%:%TAG%
if errorlevel 1 goto :error

echo Done: %IMAGE_NAME%:%TAG%
goto :eof

:error
echo Error during build or push.
exit /b 1