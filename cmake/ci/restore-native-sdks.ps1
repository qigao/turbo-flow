param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("linux-x64", "windows-x64")]
  [string]$Rid,
  [switch]$WithRulesForge,
  [switch]$WithTurboDB
)

$ErrorActionPreference = "Stop"

foreach ($name in @("GITHUB_TOKEN", "RUNNER_TEMP", "GITHUB_ENV", "GITHUB_PATH")) {
  $value = [Environment]::GetEnvironmentVariable($name)
  if ([string]::IsNullOrWhiteSpace($value)) { throw "$name is required" }
}

$packages = if ($env:QIGAO_NUGET_PACKAGES) {
  $env:QIGAO_NUGET_PACKAGES
} else {
  Join-Path $env:RUNNER_TEMP "qigao-nuget"
}
$config = Join-Path $env:RUNNER_TEMP "turboflow-native-sdk.config"
$project = Join-Path $env:RUNNER_TEMP "turboflow-native-sdk.csproj"
$assetsPath = Join-Path $env:RUNNER_TEMP "obj/project.assets.json"

@'
<?xml version="1.0" encoding="utf-8"?>
<configuration><packageSources><clear /></packageSources></configuration>
'@ | Set-Content -LiteralPath $config -Encoding utf8NoBOM

$sourceArgs = @(
  "nuget", "add", "source", "https://nuget.pkg.github.com/qigao/index.json",
  "--name", "github",
  "--username", "qigao",
  "--password", $env:GITHUB_TOKEN,
  "--store-password-in-clear-text",
  "--configfile", $config
)
& dotnet @sourceArgs
if ($LASTEXITCODE -ne 0) { throw "failed to configure qigao GitHub Packages source" }

# Floating '*' is intentional: CI always validates the latest published stable
# producer SDKs. Compatibility is enforced by exported targets and ABI checks,
# not by hard-coded package version numbers.
$refs = @(
  '    <PackageReference Include="Salts.Native" Version="*" />',
  '    <PackageReference Include="SaltsUtils.Native" Version="*" />'
)
if ($WithRulesForge) {
  $refs += '    <PackageReference Include="RulesForge.Native" Version="*" />'
}
if ($WithTurboDB) {
  $refs += '    <PackageReference Include="TurboDB.Native" Version="*" />'
}

$refText = $refs -join [Environment]::NewLine
@"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <RestorePackagesWithLockFile>false</RestorePackagesWithLockFile>
  </PropertyGroup>
  <ItemGroup>
$refText
  </ItemGroup>
</Project>
"@ | Set-Content -LiteralPath $project -Encoding utf8NoBOM

& dotnet restore $project --packages $packages --configfile $config --no-cache --force-evaluate
if ($LASTEXITCODE -ne 0) { throw "failed to restore latest published TurboFlow producer SDKs" }
if (-not (Test-Path -LiteralPath $assetsPath -PathType Leaf)) {
  throw "NuGet restore did not produce $assetsPath"
}

$assets = Get-Content -LiteralPath $assetsPath -Raw | ConvertFrom-Json

function Get-ResolvedPackageVersion([string]$PackageId) {
  $prefix = "$PackageId/"
  foreach ($property in $assets.libraries.PSObject.Properties) {
    if ($property.Name.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
      return $property.Name.Substring($prefix.Length)
    }
  }
  throw "NuGet restore did not resolve $PackageId"
}

function Get-NativeSdk([string]$PackageId) {
  $resolvedVersion = Get-ResolvedPackageVersion $PackageId
  $packageFolder = $PackageId.ToLowerInvariant()
  $root = Join-Path $packages "$packageFolder/$resolvedVersion/sdk/$Rid"
  return [PSCustomObject]@{
    Id = $PackageId
    Version = $resolvedVersion
    Root = $root
  }
}

$salts = Get-NativeSdk "Salts.Native"
$saltsUtils = Get-NativeSdk "SaltsUtils.Native"
$saltsRoot = $salts.Root
$saltsUtilsRoot = $saltsUtils.Root

$required = @(
  (Join-Path $saltsRoot "lib/cmake/Salts/SaltsConfig.cmake"),
  (Join-Path $saltsUtilsRoot "lib/cmake/SaltsUtils/SaltsUtilsConfig.cmake"),
  (Join-Path $saltsUtilsRoot "include/data_bind.h")
)
$idlc = if ($Rid -eq "windows-x64") {
  Join-Path $saltsUtilsRoot "bin/salts-idlc.exe"
} else {
  Join-Path $saltsUtilsRoot "bin/salts-idlc"
}
$required += $idlc

$rulesForge = $null
$rulesForgeRoot = $null
if ($WithRulesForge) {
  $rulesForge = Get-NativeSdk "RulesForge.Native"
  $rulesForgeRoot = $rulesForge.Root
  $required += (Join-Path $rulesForgeRoot "lib/cmake/RulesForge/RulesForgeConfig.cmake")
  $required += (Join-Path $rulesForgeRoot "include/rules_forge.h")
}

$turboDb = $null
$turboDbRoot = $null
if ($WithTurboDB) {
  $turboDb = Get-NativeSdk "TurboDB.Native"
  $turboDbRoot = $turboDb.Root
  $required += (Join-Path $turboDbRoot "lib/cmake/Orm/OrmConfig.cmake")
}

foreach ($path in $required) {
  if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
    throw "published SDK is incomplete: $path"
  }
}

# Keep capability/ABI checks even though package versions float.
$dataBindHeader = Get-Content -LiteralPath (Join-Path $saltsUtilsRoot "include/data_bind.h") -Raw
if ($dataBindHeader -notmatch '#define\s+DATA_BIND_VERSION_MAJOR\s+3') {
  throw "latest SaltsUtils.Native does not expose DataBind 3"
}
if ($dataBindHeader -notmatch '#define\s+DATA_BIND_ABI_VERSION\s+9') {
  throw "latest SaltsUtils.Native does not expose DataBind ABI 9"
}

"SALTS_ROOT=$saltsRoot" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
"SALTS_UTILS_ROOT=$saltsUtilsRoot" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
"SALTS_UTILS_HOST_ROOT=$saltsUtilsRoot" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
"QIGAO_NUGET_PACKAGES=$packages" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
if ($WithRulesForge) {
  "RULES_FORGE_ROOT=$rulesForgeRoot" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
}
if ($WithTurboDB) {
  "TURBODB_ROOT=$turboDbRoot" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
}

(Join-Path $saltsRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
(Join-Path $saltsUtilsRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
if ($WithRulesForge) {
  (Join-Path $rulesForgeRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
}
if ($WithTurboDB) {
  (Join-Path $turboDbRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
}

if ($Rid -eq "linux-x64") {
  $entries = @((Join-Path $saltsRoot "lib"), (Join-Path $saltsUtilsRoot "lib"))
  if ($WithRulesForge) { $entries += (Join-Path $rulesForgeRoot "lib") }
  if ($WithTurboDB) { $entries += (Join-Path $turboDbRoot "lib") }
  if (-not [string]::IsNullOrWhiteSpace($env:LD_LIBRARY_PATH)) {
    $entries += $env:LD_LIBRARY_PATH
  }
  "LD_LIBRARY_PATH=$($entries -join ':')" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
}

Write-Host "Restored latest $($salts.Id) -> $($salts.Version) -> $saltsRoot"
Write-Host "Restored latest $($saltsUtils.Id) -> $($saltsUtils.Version) -> $saltsUtilsRoot"
if ($WithRulesForge) {
  Write-Host "Restored latest $($rulesForge.Id) -> $($rulesForge.Version) -> $rulesForgeRoot"
}
if ($WithTurboDB) {
  Write-Host "Restored latest $($turboDb.Id) -> $($turboDb.Version) -> $turboDbRoot"
}
