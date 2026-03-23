@echo off
setlocal enabledelayedexpansion

REM ====== Start v adresari kde lezi tento BAT ======
set "HERE=%~dp0"
cd /d "%HERE%"

REM ====== Najdi projekt root ======
REM Varianta 1: BAT je v rootu a tools\run_ui.py existuje
set "ROOT=%HERE%"
if exist "%ROOT%tools\run_ui.py" goto :root_ok

REM Varianta 2: BAT je v tools\ a root je o uroven vys
set "ROOT=%HERE%..\\"
if exist "%ROOT%tools\run_ui.py" goto :root_ok

echo [ERROR] Nemuzu najit tools\run_ui.py ani v %HERE% ani o uroven vys.
echo         Dej run_app.bat do rootu projektu nebo do tools\.
pause
exit /b 1

:root_ok
cd /d "%ROOT%"

set "VENV_DIR=.venv"
set "PY=%ROOT%%VENV_DIR%\Scripts\python.exe"
set "PIP=%ROOT%%VENV_DIR%\Scripts\pip.exe"
set "STREAMLIT=%ROOT%%VENV_DIR%\Scripts\streamlit.exe"
set "APP=%ROOT%tools\run_ui.py"
set "REQ=%ROOT%requirements.txt"

echo.
echo [INFO] ROOT = %ROOT%
echo [INFO] APP  = %APP%
echo.

REM ====== Over, ze existuje python na PATH (pro vytvoreni venv) ======
where python >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Prikaz "python" neni na PATH.
  echo         Reseni:
  echo           - otevri CMD/PowerShell a zkus:  python --version
  echo           - pokud to nejde, pridej Python do PATH (nebo nainstaluj klasicky Python.org)
  echo           - MS Store Python miva bordel s PATH/launcherem
  pause
  exit /b 1
)

REM ====== Vytvor venv, pokud neexistuje ======
if not exist "%PY%" (
  echo [INFO] Venv nenalezen. Vytvarim %VENV_DIR% ...
  python -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo [ERROR] Nepodarilo se vytvorit venv.
    pause
    exit /b 1
  )
)

REM ====== Upgrade pip ======
echo [INFO] Upgrade pip...
"%PY%" -m pip install --upgrade pip >nul 2>&1

REM ====== Instalace requirements (pokud existuje) ======
if exist "%REQ%" (
  echo [INFO] Instaluju requirements.txt (pokud je co)...
  "%PIP%" install -r "%REQ%"
  if errorlevel 1 (
    echo [ERROR] Instalace requirements selhala.
    pause
    exit /b 1
  )
) else (
  echo [WARN] requirements.txt nenalezen. Preskakuju instalaci baliku.
)

REM ====== Spust Streamlit ======
if not exist "%APP%" (
  echo [ERROR] Nenalezeno: %APP%
  pause
  exit /b 1
)

echo.
echo [INFO] Spoustim Streamlit...
echo.

"%STREAMLIT%" run "%APP%"
set "RC=%ERRORLEVEL%"

echo.
echo [INFO] Streamlit skoncil s rc=%RC%
pause
exit /b %RC%
