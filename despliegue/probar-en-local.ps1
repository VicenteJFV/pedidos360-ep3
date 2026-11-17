<#
.SINOPSIS
    Levanta en Docker Desktop el mismo despliegue de la EC2 (EP1 + EP2 + EP3),
    para ensayarlo sin gastar creditos del Learner Lab.

.DESCRIPCION
    Usa despliegue/docker-compose.yml, el archivo real de la nube, mas
    docker-compose.local.yml, que cambia Oracle por la base en memoria. Genera
    las credenciales locales que en la EC2 genera instalar-en-ec2.sh (cookie y
    usuario de RabbitMQ, usuario de Kafka UI) y construye las ocho imagenes.

    Antes detiene los clusters de laboratorio (rabbitmq-cluster y
    kafka-cluster), que usan los mismos nombres de contenedor y puertos. Sus
    datos se conservan.

    La primera construccion tarda varios minutos: Maven y Node descargan sus
    dependencias dentro de las imagenes.

.EJEMPLO
    .\probar-en-local.ps1

.EJEMPLO
    # Detiene y elimina los contenedores del ensayo (conserva los volumenes)
    .\probar-en-local.ps1 -Detener
#>
[CmdletBinding()]
param(
    [switch]$Detener,
    [switch]$SinConstruir
)

$ErrorActionPreference = "Stop"
$raiz = Split-Path $PSScriptRoot -Parent
$compose = @("compose", "--project-directory", $PSScriptRoot,
             "-f", (Join-Path $PSScriptRoot "docker-compose.yml"),
             "-f", (Join-Path $PSScriptRoot "docker-compose.local.yml"))

function Invoke-Nativo {
    param([Parameter(Mandatory)][string[]]$Argumentos, [switch]$Mostrar)
    $previo = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $resto = @(); if ($Argumentos.Count -gt 1) { $resto = $Argumentos[1..($Argumentos.Count - 1)] }
        if ($Mostrar) { & $Argumentos[0] @resto 2>&1 | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }; $script:codigo = $LASTEXITCODE; return }
        $salida = & $Argumentos[0] @resto 2>&1 | Out-String
        $script:codigo = $LASTEXITCODE
        return $salida
    } finally { $ErrorActionPreference = $previo }
}

function Paso([string]$Texto) { Write-Host ""; Write-Host "=== $Texto ===" -ForegroundColor Cyan }

function Hex([int]$Bytes) {
    $b = New-Object byte[] $Bytes
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($b)
    return -join ($b | ForEach-Object { $_.ToString("x2") })
}

Invoke-Nativo @("docker", "info") | Out-Null
if ($script:codigo -ne 0) { throw "Docker no responde. Abre Docker Desktop y vuelve a intentar." }

if ($Detener) {
    Invoke-Nativo (@("docker") + $compose + @("down")) -Mostrar
    Write-Host "Ensayo detenido. Los volumenes se conservan." -ForegroundColor Green
    return
}

Paso "Clusters de laboratorio"
foreach ($carpeta in "rabbitmq-cluster", "kafka-cluster") {
    Invoke-Nativo @("docker", "compose", "--project-directory", (Join-Path $raiz $carpeta), "down") | Out-Null
    Write-Host "  $carpeta detenido (sus datos se conservan)" -ForegroundColor DarkGray
}

Paso "Credenciales locales"
# Lo mismo que genera instalar-en-ec2.sh en la nube. Git ignora estos archivos.
$utf8 = New-Object Text.UTF8Encoding($false)
$env = Join-Path $PSScriptRoot ".env"
if (-not (Test-Path $env)) {
    $lineas = @("RABBITMQ_COOKIE=$(Hex 24)", "RABBITMQ_USER=pedidos360", "RABBITMQ_PASS=$(Hex 16)",
                "KAFKAUI_USER=pedidos360", "KAFKAUI_PASS=$(Hex 16)")
    [IO.File]::WriteAllText($env, (($lineas -join "`n") + "`n"), $utf8)
    Write-Host "  .env generado" -ForegroundColor Green
} else {
    Write-Host "  .env conservado" -ForegroundColor DarkGray
}
$valores = @{}
Get-Content $env | Where-Object { $_ -match "^(\w+)=(.*)$" } | ForEach-Object { $valores[$Matches[1]] = $Matches[2] }
[IO.File]::WriteAllText((Join-Path $PSScriptRoot "rabbitmq-usuario.conf"),
    "default_user = $($valores.RABBITMQ_USER)`ndefault_pass = $($valores.RABBITMQ_PASS)`n", $utf8)
# El Compose exige que oracle.env exista. Con el perfil dev nadie lo lee: los
# valores son de relleno, no una credencial.
$oracle = Join-Path $PSScriptRoot "oracle.env"
if (-not (Test-Path $oracle)) {
    [IO.File]::WriteAllText($oracle, "ORACLE_USER=sin-uso-en-local`nORACLE_PASSWORD=sin-uso-en-local`n", $utf8)
}

if (-not $SinConstruir) {
    Paso "Imagenes"
    $imagenes = [ordered]@{
        catalog = "ms-pedidos360-catalog"; orders = "ms-pedidos360-orders"; notify = "ms-pedidos360-notify"
        rabbitadmin = "ms-pedidos360-rabbitadmin"; reportes = "ms-pedidos360-reportes"
        kafkaadmin = "ms-pedidos360-kafkaadmin"; bff = "bff"; frontend = "frontend"
    }
    foreach ($nombre in $imagenes.Keys) {
        Write-Host "  construyendo pedidos360/${nombre}:ep2..." -NoNewline
        $salida = Invoke-Nativo @("docker", "build", "-q", "-t", "pedidos360/${nombre}:ep2", (Join-Path $raiz $imagenes[$nombre]))
        if ($script:codigo -ne 0) { Write-Host ""; Write-Host $salida -ForegroundColor DarkGray; throw "No se pudo construir $nombre." }
        Write-Host " lista" -ForegroundColor Green
    }
}

Paso "Contenedores"
Invoke-Nativo (@("docker") + $compose + @("up", "-d", "--remove-orphans")) -Mostrar
if ($script:codigo -ne 0) { throw "docker compose up fallo." }

Paso "Cluster RabbitMQ"
foreach ($nodo in "rabbitmq-1", "rabbitmq-2") {
    for ($i = 0; $i -lt 30; $i++) {
        Invoke-Nativo @("docker", "exec", $nodo, "rabbitmq-diagnostics", "-q", "ping") | Out-Null
        if ($script:codigo -eq 0) { break }
        Start-Sleep -Seconds 5
    }
}
$estado = Invoke-Nativo @("docker", "exec", "rabbitmq-1", "rabbitmqctl", "cluster_status")
if ($estado -notmatch "rabbit@rabbitmq-2") {
    Invoke-Nativo @("docker", "exec", "rabbitmq-2", "rabbitmqctl", "stop_app") | Out-Null
    Invoke-Nativo @("docker", "exec", "rabbitmq-2", "rabbitmqctl", "join_cluster", "rabbit@rabbitmq-1") | Out-Null
    Invoke-Nativo @("docker", "exec", "rabbitmq-2", "rabbitmqctl", "start_app") | Out-Null
    Write-Host "  rabbitmq-2 unido al cluster de rabbitmq-1" -ForegroundColor Green
} else {
    Write-Host "  el cluster ya estaba formado" -ForegroundColor DarkGray
}

Paso "Cluster Kafka"
$init = ""
for ($i = 0; $i -lt 60; $i++) {
    $init = (Invoke-Nativo @("docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", "kafka-init")).Trim()
    if ($init -like "exited *") { break }
    Start-Sleep -Seconds 5
}
if ($init -ne "exited 0") { throw "Los topicos no se crearon (kafka-init: $init). Revisa: docker logs kafka-init" }
$descripcion = Invoke-Nativo @("docker", "exec", "kafka-1", "kafka-topics", "--bootstrap-server", "kafka-1:9092", "--describe")
($descripcion -split "`r?`n") | Where-Object { $_ -match "^Topic: pedidos-(eventos|auditoria)\s" } |
    ForEach-Object { Write-Host ("  " + ($_ -replace "TopicId: \S+\s+", "" -replace "Configs:.*", "")) -ForegroundColor Green }

Paso "Salud de los servicios"
$pruebas = [ordered]@{
    "catalog"     = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8082/actuator/health" -TimeoutSec 5).StatusCode -eq 200 }
    "orders"      = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8081/actuator/health" -TimeoutSec 5).StatusCode -eq 200 }
    "notify"      = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8083/actuator/health" -TimeoutSec 5).StatusCode -eq 200 }
    "rabbitadmin" = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8084/actuator/health" -TimeoutSec 5).StatusCode -eq 200 }
    "reportes"    = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8085/actuator/health" -TimeoutSec 5).StatusCode -eq 200 }
    "kafkaadmin"  = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8086/actuator/health" -TimeoutSec 5).StatusCode -eq 200 }
    # El BFF vivo responde 401: exige el JWT, que es lo correcto.
    "bff"         = { try { Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8080/api/catalog" -TimeoutSec 5 | Out-Null; $false } catch { $_.Exception.Response.StatusCode.value__ -eq 401 } }
    "frontend"    = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1/app/" -TimeoutSec 5).StatusCode -eq 200 }
    "kafka-ui"    = { (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:8090/" -TimeoutSec 5).StatusCode -eq 200 }
}
$pendientes = @($pruebas.Keys)
for ($i = 0; $i -lt 36 -and $pendientes.Count -gt 0; $i++) {
    $siguen = @()
    foreach ($nombre in $pendientes) {
        $ok = $false
        try { $ok = [bool](& $pruebas[$nombre]) } catch { $ok = $false }
        if (-not $ok) { $siguen += $nombre }
    }
    $pendientes = $siguen
    if ($pendientes.Count -gt 0) { Start-Sleep -Seconds 5 }
}
foreach ($nombre in $pruebas.Keys) {
    if ($pendientes -contains $nombre) { Write-Host "  [FALLA] $nombre" -ForegroundColor Red }
    else { Write-Host "  [OK]    $nombre" -ForegroundColor Green }
}

Write-Host ""
Write-Host "  Kafka UI:            http://localhost:8090   usuario $($valores.KAFKAUI_USER)" -ForegroundColor Cyan
Write-Host "  RabbitMQ Management: http://localhost:15672  usuario $($valores.RABBITMQ_USER)" -ForegroundColor Cyan
Write-Host "  Las contrasenas estan en despliegue\.env (git lo ignora)." -ForegroundColor DarkGray
Write-Host "  Para detener el ensayo:  .\probar-en-local.ps1 -Detener" -ForegroundColor DarkGray
Write-Host ""
if ($pendientes.Count -gt 0) { exit 1 }
