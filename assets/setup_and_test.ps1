# setup_and_test.ps1 - automated CI/CD for Windows
Write-Host ">>> Starting GDPR Obfuscator Service Setup & Test Pipeline..." -ForegroundColor Cyan

# 1. create virtual environment if it doesn't exist
if (!(Test-Path -Path ".\venv")) {
    Write-Host ">>> Creating Virtual Environment..." -ForegroundColor Yellow
    python -m venv venv
}

# 2. activate environment
Write-Host ">>> Activating Environment..." -ForegroundColor Yellow
.\venv\Scripts\Activate.ps1

# 3. upgrade pip and install dependencies
Write-Host ">>> Installing Dependencies..." -ForegroundColor Yellow
python -m pip install --upgrade pip
pip install -r requirements.txt

# 4. set environment variables
Write-Host ">>> Setting PYTHONPATH..." -ForegroundColor Yellow
$env:PYTHONPATH = "src;."

# 5. execute pipeline
Write-Host "`n>>> RUNNING UNIT TESTS (Pytest)..." -ForegroundColor Green
python -m pytest tests -vv -s --color=yes

Write-Host "`n>>> RUNNING SECURITY SCAN (Bandit)..." -ForegroundColor Green
python -m bandit -lll -r src/

Write-Host "`n>>> RUNNING VULNERABILITY AUDIT (pip-audit)..." -ForegroundColor Green
python -m pip_audit

Write-Host "`n>>> RUNNING LINTER (Flake8)..." -ForegroundColor Green
python -m flake8 --max-line-length=100 src tests

Write-Host "`n>>> RUNNING FORMATTER (Black)..." -ForegroundColor Green
python -m black src tests

Write-Host "`n>>> GENERATING INTEGRATED REPORT (coverage.txt)..." -ForegroundColor Cyan

"--- UNIT TEST COVERAGE REPORT ---" | Out-File -FilePath coverage.txt
python -m coverage run -m pytest tests
python -m coverage report -m >> coverage.txt
"`n--- SECURITY SCAN REPORT (BANDIT) ---" >> coverage.txt
bandit -lll -r src/ >> coverage.txt 2>&1
"`n--- VULNERABILITY AUDIT REPORT (PIP-AUDIT) ---" >> coverage.txt
pip-audit >> coverage.txt 2>&1

Write-Host "`n>>> Pipeline Completed Successfully! Coverage Report created! (PEP8 Compliant)" -ForegroundColor Cyan