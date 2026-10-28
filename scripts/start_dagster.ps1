# Windows entry point: picks a real Linux distro (skips Docker's) and runs scripts/run_dagster.sh in it.
$repo = Split-Path -Parent $PSScriptRoot
$distros = (wsl.exe -l -q) -replace "`0", '' | ForEach-Object { $_.Trim() } |
  Where-Object { $_ -and $_ -notmatch '^docker' }
$d = $distros | Where-Object { $_ -match 'Ubuntu' } | Select-Object -First 1
if (-not $d) { $d = $distros | Select-Object -First 1 }
if (-not $d) { Write-Host "No Linux distro in WSL. Run: wsl --install -d Ubuntu, open Ubuntu once, then retry."; exit 1 }
$wslRepo = (wsl.exe -d $d -e wslpath -a ($repo -replace '\\', '/')).Trim()
Write-Host "Using WSL distro: $d   repo: $wslRepo"
wsl.exe -d $d --cd $wslRepo -e bash scripts/run_dagster.sh
