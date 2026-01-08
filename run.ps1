$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root
if (!(Test-Path ".venv")) { py -3 -m venv .venv }
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Write-Host "`nAPI prête !`n" -ForegroundColor Green
Write-Host "Docs : http://127.0.0.1:8000/docs"
Write-Host "Health : http://127.0.0.1:8000/health`n"
uvicorn main:app --reload --host 0.0.0.0 --port 8000
