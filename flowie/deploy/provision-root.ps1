[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [uri]$ControlUrl,

    [Parameter(Mandatory = $true)]
    [string]$RootGroup,

    [Parameter(Mandatory = $true)]
    [string]$AdminPrincipal,

    [string]$SystemPrincipal = "admin",

    [string]$SystemPasswordEnv = "FLOWIE_SYSTEM_ADMIN_PASSWORD",

    [string]$AdminPasswordEnv = "FLOWIE_ROOT_ADMIN_PASSWORD",

    [switch]$AllowInsecureLocalHttp
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$managementRoot = "system"
$managementLoginPath = "/v2/control/login"
$managementRpcPath = "/v2/control/rpc"
$managementLogoutPath = "/v2/control/logout"
$securityAdminRole = "security_admin"
$minimumPasswordLength = 16
$maximumPasswordLength = 4096
$maximumSecurityIdLength = 255
$rpcId = 0L

function Assert-SecurityId {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    if ([string]::IsNullOrEmpty($Value) -or
        $Value.Length -gt $maximumSecurityIdLength -or
        $Value -match "[\x00-\x1f\x7f]") {
        throw "$Name must contain 1..$maximumSecurityIdLength non-control characters"
    }
}

function Get-RequiredSecret {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name
    )

    if ($Name -notmatch "^[A-Za-z_][A-Za-z0-9_]*$") {
        throw "Invalid secret environment variable name"
    }

    $value = [Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrEmpty($value)) {
        throw "Required secret environment variable is not set: $Name"
    }
    if ($value.Length -lt $minimumPasswordLength -or $value.Length -gt $maximumPasswordLength) {
        throw "$Name must contain $minimumPasswordLength..$maximumPasswordLength characters"
    }
    return $value
}

function Get-ProvisionKey {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Value
    )

    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value)
        $digest = $sha256.ComputeHash($bytes)
        return ([Convert]::ToHexString($digest).Substring(0, 16)).ToLowerInvariant()
    }
    finally {
        $sha256.Dispose()
    }
}

function New-ManagementSession {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RootGroupId,

        [Parameter(Mandatory = $true)]
        [string]$PrincipalId,

        [Parameter(Mandatory = $true)]
        [string]$Password
    )

    $session = [Microsoft.PowerShell.Commands.WebRequestSession]::new()
    $body = @{
        domain = $RootGroupId
        principal  = $PrincipalId
        password   = $Password
    }
    $response = Invoke-WebRequest `
        -Uri "$origin$managementLoginPath" `
        -Method Post `
        -ContentType "application/x-www-form-urlencoded" `
        -Headers @{ Origin = $origin } `
        -Body $body `
        -WebSession $session `
        -MaximumRedirection 0 `
        -SkipHttpErrorCheck `
        -ErrorAction SilentlyContinue
    if ($response.StatusCode -ne 303) {
        throw "Flowie management login failed with HTTP status $($response.StatusCode)"
    }
    return $session
}

function Invoke-ManagementRpc {
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.PowerShell.Commands.WebRequestSession]$Session,

        [Parameter(Mandatory = $true)]
        [string]$Method,

        [Parameter(Mandatory = $true)]
        [hashtable]$Params
    )

    $script:rpcId++
    $request = @{
        jsonrpc = "2.0"
        method  = $Method
        params  = $Params
        id      = $script:rpcId
    } | ConvertTo-Json -Depth 8 -Compress
    $response = Invoke-WebRequest `
        -Uri "$origin$managementRpcPath" `
        -Method Post `
        -ContentType "application/json" `
        -Body $request `
        -WebSession $Session `
        -SkipHttpErrorCheck
    if ($response.StatusCode -ne 200) {
        throw "$Method failed with HTTP status $($response.StatusCode)"
    }

    $document = $response.Content | ConvertFrom-Json
    $errorProperty = $document.PSObject.Properties["error"]
    if ($null -ne $errorProperty -and $null -ne $errorProperty.Value) {
        $exception = [System.InvalidOperationException]::new(
            "$Method failed with RPC code $($errorProperty.Value.code): $($errorProperty.Value.message)"
        )
        $exception.Data["FlowieRpcCode"] = [long]$errorProperty.Value.code
        throw $exception
    }
    $resultProperty = $document.PSObject.Properties["result"]
    if ($null -eq $resultProperty -or $null -eq $resultProperty.Value) {
        throw "$Method returned no result"
    }
    return $resultProperty.Value
}

function Invoke-ProvisionStep {
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.PowerShell.Commands.WebRequestSession]$Session,

        [Parameter(Mandatory = $true)]
        [string]$Method,

        [Parameter(Mandatory = $true)]
        [hashtable]$Params,

        [switch]$DeferCredentialCreateConflict
    )

    if ($DeferCredentialCreateConflict -and
        ($Method -ne "control.password.set" -or $Params.mode -ne "create")) {
        throw "Deferred conflict verification is restricted to control.password.set mode=create"
    }

    $commandParams = @{}
    foreach ($entry in $Params.GetEnumerator()) {
        $commandParams[$entry.Key] = $entry.Value
    }
    try {
        $result = Invoke-ManagementRpc -Session $Session -Method $Method -Params $commandParams
    }
    catch {
        if ($DeferCredentialCreateConflict -and
            $_.Exception.Data["FlowieRpcCode"] -eq -32009) {
            Write-Host "$Method existing credential deferred to final authentication verification"
            return
        }
        throw
    }
    if ($null -eq $result.replayed) {
        throw "$Method returned an invalid command result"
    }
    Write-Host "$Method replayed=$($result.replayed)"
}

function Close-ManagementSession {
    param(
        [Microsoft.PowerShell.Commands.WebRequestSession]$Session
    )

    if ($null -eq $Session) {
        return
    }
    try {
        Invoke-WebRequest `
            -Uri "$origin$managementLogoutPath" `
            -Method Post `
            -Headers @{ Origin = $origin } `
            -WebSession $Session `
            -MaximumRedirection 0 `
            -SkipHttpErrorCheck `
            -ErrorAction SilentlyContinue | Out-Null
    }
    catch {
        Write-Warning "Flowie management logout did not complete"
    }
}

Assert-SecurityId -Name "RootGroup" -Value $RootGroup
Assert-SecurityId -Name "AdminPrincipal" -Value $AdminPrincipal
Assert-SecurityId -Name "SystemPrincipal" -Value $SystemPrincipal

if (-not $ControlUrl.IsAbsoluteUri) {
    throw "ControlUrl must be an absolute URL"
}
if ($ControlUrl.UserInfo -or $ControlUrl.Query -or $ControlUrl.Fragment -or
    ($ControlUrl.AbsolutePath -ne "/" -and $ControlUrl.AbsolutePath -ne "")) {
    throw "ControlUrl must contain only scheme and authority"
}
if ($ControlUrl.Scheme -ne "https") {
    $localHosts = @("127.0.0.1", "::1", "localhost")
    if (-not $AllowInsecureLocalHttp -or
        $ControlUrl.Scheme -ne "http" -or
        $ControlUrl.Host -notin $localHosts) {
        throw "ControlUrl must use HTTPS; insecure HTTP is restricted to an explicit local endpoint"
    }
}

$origin = $ControlUrl.GetLeftPart([System.UriPartial]::Authority)
$systemPassword = $null
$adminPassword = $null
$systemSession = $null
$adminSession = $null

try {
    $systemPassword = Get-RequiredSecret -Name $SystemPasswordEnv
    $adminPassword = Get-RequiredSecret -Name $AdminPasswordEnv
    $provisionKey = Get-ProvisionKey -Value "$SystemPrincipal|$RootGroup|$AdminPrincipal"
    $requestPrefix = "root-provision-v1-$provisionKey"

    $systemSession = New-ManagementSession `
        -RootGroupId $managementRoot `
        -PrincipalId $SystemPrincipal `
        -Password $systemPassword

    Invoke-ProvisionStep -Session $systemSession -Method "control.domain.create" -Params @{
        domain_id = $RootGroup
        request_id    = "$requestPrefix-root"
    }
    Invoke-ProvisionStep -Session $systemSession -Method "control.user.create" -Params @{
        domain_id  = $RootGroup
        principal_id   = $AdminPrincipal
        principal_type = "human"
        request_id     = "$requestPrefix-user"
    }
    Invoke-ProvisionStep -Session $systemSession -Method "control.password.set" -Params @{
        domain_id = $RootGroup
        principal_id  = $AdminPrincipal
        new_password  = $adminPassword
        mode          = "create"
        request_id    = "$requestPrefix-password"
    } -DeferCredentialCreateConflict
    Invoke-ProvisionStep -Session $systemSession -Method "control.role.create" -Params @{
        domain_id = $RootGroup
        role_id       = $securityAdminRole
        request_id    = "$requestPrefix-role"
    }
    Invoke-ProvisionStep -Session $systemSession -Method "control.role.assign" -Params @{
        domain_id = $RootGroup
        principal_id  = $AdminPrincipal
        role_id       = $securityAdminRole
        request_id    = "$requestPrefix-assignment"
    }

    $adminSession = New-ManagementSession `
        -RootGroupId $RootGroup `
        -PrincipalId $AdminPrincipal `
        -Password $adminPassword
    $adminStatus = Invoke-ManagementRpc `
        -Session $adminSession `
        -Method "control.system.status" `
        -Params @{}
    if ($adminStatus.domain -ne $RootGroup) {
        throw "Provisioned administrator resolved to an unexpected Domain"
    }

    Write-Host "Provisioned and verified $RootGroup/$AdminPrincipal through Flowie management RPC"
}
finally {
    Close-ManagementSession -Session $adminSession
    Close-ManagementSession -Session $systemSession
    $adminPassword = $null
    $systemPassword = $null
}
