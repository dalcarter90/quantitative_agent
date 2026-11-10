<#
.SYNOPSIS
  Build everything and run every test. A phase is done only when this passes.

.DESCRIPTION
  1. Creates .venv (if missing) and installs the Python package with dev extras.
  2. Configures and builds cpp/ (vcpkg manifest restores GoogleTest/Benchmark).
  3. Runs the C++ tests (ctest), then the Python tests (pytest), then ruff.

  Requires: Visual Studio 2022 Build Tools (C++ workload), CMake >= 3.25,
  Python >= 3.11, and VCPKG_ROOT pointing at a bootstrapped vcpkg clone.

.PARAMETER Config
  Release (default) or Debug.
#>
param(
    [ValidateSet('Release', 'Debug')]
    [string]$Config = 'Release'
)

$ErrorActionPreference = 'Stop'
$repo = Split-Path -Parent $PSScriptRoot

function Invoke-Step {
    param([string]$Name, [scriptblock]$Body)
    Write-Host "==> $Name" -ForegroundColor Cyan
    & $Body
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $Name (exit $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

if (-not $env:VCPKG_ROOT) {
    Write-Host 'VCPKG_ROOT is not set. See CLAUDE.md, "One-time setup".' -ForegroundColor Red
    exit 1
}

$python = Join-Path $repo '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) {
    Invoke-Step 'Create .venv' { python -m venv (Join-Path $repo '.venv') }
}

$preset = "windows-$($Config.ToLower())"

Invoke-Step 'Install Python package (editable, dev extras)' {
    & $python -m pip install --quiet --disable-pip-version-check -e "$repo\python[dev]"
}
# CMake presets are resolved relative to the current directory.
Push-Location (Join-Path $repo 'cpp')
Invoke-Step 'CMake configure' { cmake --preset windows }
Invoke-Step "CMake build ($Config)" { cmake --build --preset $preset }
Invoke-Step 'C++ tests' { ctest --preset $preset }
Pop-Location
Invoke-Step 'Python tests' { & $python -m pytest "$repo\python" }
Invoke-Step 'Ruff' { & $python -m ruff check "$repo\python" }

Write-Host 'ALL GREEN' -ForegroundColor Green
