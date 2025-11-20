@echo off
echo ==================================================
echo    GLOBAL SERVER MONITORING SYSTEM - LAUNCHER
echo ==================================================

echo.
echo [Step 1/4] Starting Generator...
echo (Creating Source Files and Opening Socket Port 9999)
start "1. Generator (Source)" cmd /k "python generate_source.py"

echo.
echo ... Waiting 3 seconds for files to generate ...
timeout /t 3 >nul

echo.
echo [Step 2/4] Running Batch ETL Pipeline...
echo (Loading 30,000 Historical Records into PostgreSQL)
:: We use /k so the window stays open and you can see the "SUCCESS" message
start "2. Batch ETL (History Layer)" cmd /k "python etl_pipeline.py"

echo.
echo ... Waiting 10 seconds for Database Load to finish ...
:: Adjust this time if your laptop is slower/faster
timeout /t 10 >nul

echo.
echo [Step 3/4] Starting Real-Time Streamer...
echo (Connecting to Port 9999 and pushing live data)
start "3. Streamer (Speed Layer)" cmd /k "python streaming_pipeline.py"

echo.
echo [Step 4/4] Launching Dashboard...
start "4. Dashboard" cmd /k "python -m streamlit run dashboard.py"

echo.
echo ==================================================
echo    SYSTEM STARTED SUCCESSFULLY! 🚀
echo ==================================================
pause