# check if wkhtmltox is already installed
$wkhtmltoxPath = "c:\Program Files\wkhtmltopdf\bin\"
if (Test-Path -Path $wkhtmltoxPath) {
  Write-Host "wkhtmltox already installed"
}
else {
  Write-Host "ERROR: wkhtmltox is not installed, wkhtmltox is required for pdfkit library. Download from here: https://wkhtmltopdf.org/downloads.html"
  exit 1
}

# Setup python virtual environment
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
  # fallback to python3 if python not found
  $pythonCmd = Get-Command python3 -ErrorAction SilentlyContinue
  if (-not $pythonCmd) {
    Write-Host "Python not found. Please install Python 3.6 or later."
    exit 1
  }
}
$ScriptPath = $PSScriptRoot
if (-not (Test-Path -Path $ScriptPath)) {
  Write-Host "scripts folder not found"
  exit 1
}

$venvPath = "$ScriptPath/.venv"
if (Test-Path -Path "$venvPath") {
  Write-Host "Virtual environment already exists, using this environment"
}
else {
  Write-Host "Creating virtual environment"
  Start-Process -FilePath ($pythonCmd).Source -ArgumentList "-m venv ""$venvPath""" -Wait -NoNewWindow
}
$venvPythonPath = "$venvPath/scripts/python.exe"
if (Test-Path -Path $venvPythonPath) {
  Write-Host "Windows path Python found in virtual environment"
  . "$venvPath\scripts\activate.ps1"
  # fallback to Linux venv path
}
else {
  
  $venvPythonPath = "$venvPath/bin/python"
  if (Test-Path -Path $venvPythonPath) {
    Write-Host "Linux path Python found in virtual environment"
    . "$venvPath\bin\activate.ps1"
  }
  else {
    Write-Host "Python found in virtual environment"
    . "$venvPath\Scripts\activate.ps1"
    exit 1
  }
}

# Install requirements
$requirementPath = (Get-ChildItem -Path $PSScriptRoot -Filter "requirements.txt" -Recurse).FullName
$requirementPath
if (-not (Test-Path -Path $requirementPath)) {
  Write-Host "requirements.txt not found"
  exit 1
}
else {
  Write-Host 'Installing dependencies from "requirements.txt"'
  Start-Process -FilePath $venvPythonPath -ArgumentList "-m pip install -r $requirementPath" -Wait -NoNewWindow
}

# Run prepdocs.py
$PrepdocPath = (Get-ChildItem -Path $PSScriptRoot -Filter "prepdocs.py" -Recurse).FullName
if (-not (Test-Path -Path $PrepdocPath)) {
  Write-Host "Error: prepdocs.py not found"
  exit 1
}
else {
  Write-Host 'Running "prepdocs.py"'
  Start-Process -FilePath $venvPythonPath -ArgumentList "$PrepdocPath -v" -Wait -NoNewWindow
}
