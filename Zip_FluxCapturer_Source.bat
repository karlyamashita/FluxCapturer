@echo off
setlocal EnableExtensions

rem Place this BAT in the root of the FluxCapturer repository.
set "SOURCE=%~dp0"
set "SOURCE=%SOURCE:~0,-1%"
rem Save the ZIP one directory above the repository root.
for %%I in ("%SOURCE%\..") do set "PARENT=%%~fI"
set "ZIP=%PARENT%\FluxCapturer_Source.zip"

where powershell.exe >nul 2>&1
if errorlevel 1 (
    echo ERROR: Windows PowerShell was not found.
    pause
    exit /b 1
)

set "PS_SCRIPT=%TEMP%\ZipFluxCapturer_%RANDOM%_%RANDOM%.ps1"
>"%PS_SCRIPT%" echo $ErrorActionPreference = 'Stop'
>>"%PS_SCRIPT%" echo $source = $env:SOURCE
>>"%PS_SCRIPT%" echo $zip = $env:ZIP
>>"%PS_SCRIPT%" echo $excludedDirs = @('bin','obj','.vs','.git','.idea','.vscode','TestResults','packages','node_modules','Debug','Release')
>>"%PS_SCRIPT%" echo $excludedExtensions = @('.dll','.exe','.pdb','.cache','.ilk','.ipch','.obj','.pch','.suo','.user','.log','.tmp','.zip','.7z','.rar')
>>"%PS_SCRIPT%" echo Add-Type -AssemblyName System.IO.Compression
>>"%PS_SCRIPT%" echo Add-Type -AssemblyName System.IO.Compression.FileSystem
>>"%PS_SCRIPT%" echo if (Test-Path -LiteralPath $zip) { Remove-Item -LiteralPath $zip -Force }
>>"%PS_SCRIPT%" echo $stream = [System.IO.File]::Open($zip, [System.IO.FileMode]::CreateNew)
>>"%PS_SCRIPT%" echo try {
>>"%PS_SCRIPT%" echo   $archive = New-Object System.IO.Compression.ZipArchive($stream, [System.IO.Compression.ZipArchiveMode]::Create)
>>"%PS_SCRIPT%" echo   try {
>>"%PS_SCRIPT%" echo     $count = 0
>>"%PS_SCRIPT%" echo     Get-ChildItem -LiteralPath $source -Recurse -File -Force ^| Where-Object {
>>"%PS_SCRIPT%" echo       $relative = $_.FullName.Substring($source.Length).TrimStart('\')
>>"%PS_SCRIPT%" echo       $parts = $relative -split '[\\/]'
>>"%PS_SCRIPT%" echo       $dirs = @($parts ^| Select-Object -SkipLast 1)
>>"%PS_SCRIPT%" echo       -not @($dirs ^| Where-Object { $_ -in $excludedDirs }).Count -and $_.Extension -notin $excludedExtensions -and $_.FullName -ne $zip
>>"%PS_SCRIPT%" echo     } ^| ForEach-Object {
>>"%PS_SCRIPT%" echo       $entry = $_.FullName.Substring($source.Length).TrimStart('\').Replace('\','/')
>>"%PS_SCRIPT%" echo       [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile($archive, $_.FullName, $entry, [System.IO.Compression.CompressionLevel]::Optimal) ^| Out-Null
>>"%PS_SCRIPT%" echo       $count++
>>"%PS_SCRIPT%" echo     }
>>"%PS_SCRIPT%" echo     Write-Host "Added $count files."
>>"%PS_SCRIPT%" echo   } finally { $archive.Dispose() }
>>"%PS_SCRIPT%" echo } finally { $stream.Dispose() }
>>"%PS_SCRIPT%" echo Write-Host "ZIP: $zip"

set "SOURCE=%SOURCE%"
set "ZIP=%ZIP%"
echo Creating source-only ZIP from:
echo %SOURCE%
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%PS_SCRIPT%"
set "RESULT=%ERRORLEVEL%"
del "%PS_SCRIPT%" >nul 2>&1
if not "%RESULT%"=="0" (
    echo ERROR: ZIP creation failed.
    pause
    exit /b %RESULT%
)
echo.
echo ZIP created successfully.
pause
endlocal
