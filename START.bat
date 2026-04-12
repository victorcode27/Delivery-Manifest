@echo off
title Delivery Manifest System - LAN Mode

:: ── SHARE CREDENTIALS ────────────────────────────────────────────────────────
:: Credentials are stored in Windows Credential Manager — NOT in this file.
:: Run this ONCE from any cmd window (as the user who runs START.bat):
::   cmdkey /add:BRD-DESKTOP-ELV /user:BRD-DESKTOP-ELV\YourUsername /pass:YourPassword
:: To update the password later, run the same command again with the new password.
:: ─────────────────────────────────────────────────────────────────────────────

echo ============================================
echo    Delivery Manifest System (LAN MODE)
echo ============================================
echo.

:: Change to the script's directory
cd /d "%~dp0"

echo [1/4] Connecting to invoice share...
:: Drop any stale or dead session first (error suppressed — no session is fine)
net use "\\BRD-DESKTOP-ELV\storage" /delete /y > nul 2>&1
:: Reconnect using credentials stored in Windows Credential Manager
net use "\\BRD-DESKTOP-ELV\storage" /persistent:no
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Cannot connect to \\BRD-DESKTOP-ELV\storage
    echo   1. Confirm BRD-DESKTOP-ELV is powered on and reachable.
    echo   2. Confirm the 'storage' share is enabled on that machine.
    echo   3. Store credentials once via: cmdkey /add:BRD-DESKTOP-ELV /user:BRD-DESKTOP-ELV\YourUsername /pass:YourPassword
    echo.
    pause
    exit /b 1
)
echo Share connected successfully.
echo.

echo [2/4] Processing any new PDF invoices...
python invoice_processor.py
echo.

echo [3/4] Starting API server (LAN MODE)...
echo WARNING: Server will be accessible to other PCs on your network
echo.

:: Get local IP address
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /i "IPv4"') do set IP=%%a
set IP=%IP:~1%
echo Your IP Address: %IP%
echo Other PCs can access: http://%IP%:8000
echo.

:: Start the modular backend (delivery routes live here)
start "Invoice API Server" cmd /k "python -m uvicorn delivery_manifest_backend.app.main:app --host 0.0.0.0 --port 8000 --reload"

:: Wait for server to start
timeout /t 3 /nobreak > nul

echo [4/4] Opening web application...
start "" "http://localhost:8000"

echo.
echo ============================================
echo    System Started Successfully!
echo ============================================
echo.
echo - API Server running with 4 workers (supports 10-20 users)
echo - Share this URL with other PCs: http://%IP%:8000
echo - To stop: close the API Server window
echo.
echo FIREWALL REMINDER:
echo If other PCs can't connect, run: setup_firewall.bat
echo.
echo You can close this window now.
pause
