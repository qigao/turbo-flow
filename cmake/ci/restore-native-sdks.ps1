param(
  [Parameter(Mandatory = $true)]
  [ValidateSet("linux-x64", "windows-x64")]
  [string]$Rid,
  [switch]$WithRulesForge,
  [switch]$WithTurboDB,
  [switch]$WithCHttp
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
# producer SDKs. Consumer contracts are enforced by their canonical exported
# packages and targets; there is no legacy package fallback.
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
if ($WithCHttp) {
  $refs += '    <PackageReference Include="CHttp.Native" Version="*" />'
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
  $required += (Join-Path $turboDbRoot "lib/cmake/TurboDB/TurboDBConfig.cmake")
}

$cHttp = $null
$cHttpRoot = $null
if ($WithCHttp) {
  $cHttp = Get-NativeSdk "CHttp.Native"
  $cHttpRoot = $cHttp.Root
  $required += (Join-Path $cHttpRoot "lib/cmake/Chttp/ChttpConfig.cmake")
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
if ($dataBindHeader -notmatch '#define\s+DATA_BIND_ABI_VERSION\s+10') {
  throw "latest SaltsUtils.Native does not expose DataBind ABI 10"
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
if ($WithCHttp) {
  "HTTP_SERVICES_ROOT=$cHttpRoot" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
}

(Join-Path $saltsRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
(Join-Path $saltsUtilsRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
if ($WithRulesForge) {
  (Join-Path $rulesForgeRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
}
if ($WithTurboDB) {
  (Join-Path $turboDbRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
}
if ($WithCHttp) {
  (Join-Path $cHttpRoot "bin") | Add-Content -LiteralPath $env:GITHUB_PATH -Encoding utf8
}

if ($Rid -eq "linux-x64") {
  $entries = @((Join-Path $saltsRoot "lib"), (Join-Path $saltsUtilsRoot "lib"))
  if ($WithRulesForge) { $entries += (Join-Path $rulesForgeRoot "lib") }
  if ($WithTurboDB) { $entries += (Join-Path $turboDbRoot "lib") }
  if ($WithCHttp) { $entries += (Join-Path $cHttpRoot "lib") }
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
if ($WithCHttp) {
  Write-Host "Restored latest $($cHttp.Id) -> $($cHttp.Version) -> $cHttpRoot"
}

# Floating latest-stable dependencies are deliberate (TurboFlow #226). Capture
# the concrete, reproducible resolution for THIS CI run; never substitute a
# fallback SDK/root if a published dependency is missing or incompatible.
$sourceSha = (& git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $sourceSha -notmatch '^[0-9a-f]{40}
$resolution = [ordered]@{
  rid = $Rid
  source_sha = $sourceSha
  salts = [ordered]@{ package = $salts.Id; version = $salts.Version; root = $saltsRoot }
  salts_utils = [ordered]@{ package = $saltsUtils.Id; version = $saltsUtils.Version; root = $saltsUtilsRoot; data_bind_abi = 10 }
}
if ($WithRulesForge) {
  $resolution['rules_forge'] = [ordered]@{ package = $rulesForge.Id; version = $rulesForge.Version; root = $rulesForgeRoot }
}
if ($WithTurboDB) {
  $resolution['turbo_db'] = [ordered]@{ package = $turboDb.Id; version = $turboDb.Version; root = $turboDbRoot }
}
if ($WithCHttp) {
  $resolution['chttp'] = [ordered]@{ package = $cHttp.Id; version = $cHttp.Version; root = $cHttpRoot }
}
$resolution | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $resolutionPath -Encoding utf8NoBOM
"TURBO_FLOW_NATIVE_SDK_RESOLUTION=$resolutionPath" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
Write-Host "Exact native SDK resolution recorded: $resolutionPath"
Get-Content -LiteralPath $resolutionPath | Write-Host
if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_STEP_SUMMARY)) {
  @(
    "### TurboFlow native SDK resolution",
    "",
    "- Source: $sourceSha",
    "- RID: $Rid",
    "- Salts.Native: $($salts.Version)",
    "- SaltsUtils.Native: $($saltsUtils.Version) (DataBind ABI 10)"
  ) | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Encoding utf8
  if ($WithCHttp) {
    "- CHttp.Native: $($cHttp.Version)" | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Encoding utf8
  }
}
) {
  throw "could not attest exact TurboFlow source HEAD"
}
if (-not [string]::IsNullOrWhiteSpace($env:EXPECTED_SHA) -and $sourceSha -ne $env:EXPECTED_SHA) {
  throw "TurboFlow source HEAD $sourceSha does not match EXPECTED_SHA $env:EXPECTED_SHA"
}
$resolutionPath = Join-Path $env:RUNNER_TEMP "turboflow-native-sdk-resolution.json"
$resolution = [ordered]@{
  rid = $Rid
  source_sha = [Environment]::GetEnvironmentVariable("EXPECTED_SHA")
  salts = [ordered]@{ package = $salts.Id; version = $salts.Version; root = $saltsRoot }
  salts_utils = [ordered]@{ package = $saltsUtils.Id; version = $saltsUtils.Version; root = $saltsUtilsRoot; data_bind_abi = 10 }
}
if ($WithRulesForge) {
  $resolution['rules_forge'] = [ordered]@{ package = $rulesForge.Id; version = $rulesForge.Version; root = $rulesForgeRoot }
}
if ($WithTurboDB) {
  $resolution['turbo_db'] = [ordered]@{ package = $turboDb.Id; version = $turboDb.Version; root = $turboDbRoot }
}
if ($WithCHttp) {
  $resolution['chttp'] = [ordered]@{ package = $cHttp.Id; version = $cHttp.Version; root = $cHttpRoot }
}
$resolution | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $resolutionPath -Encoding utf8NoBOM
"TURBO_FLOW_NATIVE_SDK_RESOLUTION=$resolutionPath" | Add-Content -LiteralPath $env:GITHUB_ENV -Encoding utf8
Write-Host "Exact native SDK resolution recorded: $resolutionPath"
Get-Content -LiteralPath $resolutionPath | Write-Host
if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_STEP_SUMMARY)) {
  @(
    "### TurboFlow native SDK resolution",
    "",
    "- Source: $([Environment]::GetEnvironmentVariable('EXPECTED_SHA'))",
    "- RID: $Rid",
    "- Salts.Native: $($salts.Version)",
    "- SaltsUtils.Native: $($saltsUtils.Version) (DataBind ABI 10)"
  ) | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Encoding utf8
  if ($WithCHttp) {
    "- CHttp.Native: $($cHttp.Version)" | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Encoding utf8
  }
}
