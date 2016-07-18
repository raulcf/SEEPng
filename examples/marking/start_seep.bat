@echo off

REM below are the full ranges for all parameters
REM SET PARS=2,3,4,5,6,7,8,9,10 1.2f,1.4f,1.6f,1.8f,2.0f,2.2f,2.4f,2.6f 2,3,4,5,6 2,3,4,5,6,7,8,9 0.01f,0.05f,0.1f,0.15f,0.2f,0.25f 2000,4000,8000,16000,48000,96000,192000,384000 0.005f,0.01f,0.02f,0.05f,0.1f,0.15f,0.2f,0.25f 10

REM below are the short parameters, i.e., one value per parameter
SET PARS=100 4.0f 3 0.05f 4000 0.01f 1

start cmd /k E:\workspace\SEEPng\install\seep-master\bin\seep-master.bat --query.file E:\repos\15-MDF-usecase\applications\simple-event-marking-query\build\libs\simple-event-marking-query.jar --baseclass.name Base --master.port 3500 %PARS%

for /L %%i in (1,1,5) do (
	start cmd /k E:\workspace\SEEPng\install\seep-worker\bin\seep-worker.bat --worker.ip 127.0.0.1 --worker.port 350%%i --data.port 500%%i --master.ip 127.0.0.1 --master.port 3500
)
