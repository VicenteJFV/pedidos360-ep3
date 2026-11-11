<#
.SINOPSIS
    Levanta el cluster Kafka de EP3 y verifica los topicos del caso.

.DESCRIPCION
    Arranca los tres ZooKeeper, los tres brokers y Kafka UI; espera a que el
    contenedor kafka-init cree los topicos y muestra su descripcion: lider,
    replicas e ISR de cada particion. Es idempotente: sirve igual para el primer
    arranque que para recuperar el cluster despues de cerrar Docker Desktop.

.EJEMPLO
    .\levantar-kafka.ps1

.EJEMPLO
    # Borra contenedores y datos y vuelve a empezar de cero
    .\levantar-kafka.ps1 -Recrear
#>
[CmdletBinding()]
param(
    [switch]$Recrear,
    [int]$SegundosEspera = 240
)

$ErrorActionPreference = "Stop"
Push-Location $PSScriptRoot

<#
    Ejecuta un comando nativo sin que su stderr mate el script. Igual que en
    rabbitmq-cluster\levantar-cluster.ps1: docker escribe en stderr lineas
    normales y PowerShell 5.1 las convierte en errores.
#>
function Invoke-Nativo {
    param([Parameter(Mandatory)][string[]]$Argumentos)

    $previo = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $resto = @()
        if ($Argumentos.Count -gt 1) { $resto = $Argumentos[1..($Argumentos.Count - 1)] }
        $salida = & $Argumentos[0] @resto 2>&1 | Out-String
        $script:ultimoCodigo = $LASTEXITCODE
        return $salida
    } finally {
        $ErrorActionPreference = $previo
    }
}

try {
    Invoke-Nativo @("docker", "info") | Out-Null
    if ($script:ultimoCodigo -ne 0) {
        throw "Docker no responde. Abre Docker Desktop, espera a que termine de arrancar y vuelve a intentar."
    }

    if ($Recrear) {
        Write-Host "[0/3] Eliminando contenedores y datos..." -ForegroundColor Cyan
        Invoke-Nativo @("docker", "compose", "down", "-v") | Out-Null
    }

    Write-Host "[1/3] Levantando ZooKeeper, brokers y Kafka UI..." -ForegroundColor Cyan
    $salida = Invoke-Nativo @("docker", "compose", "up", "-d")
    if ($script:ultimoCodigo -ne 0) {
        Write-Host $salida -ForegroundColor DarkGray
        throw "docker compose up fallo."
    }

    # kafka-init espera a que los tres brokers esten sanos, crea los topicos y
    # termina. Su codigo de salida dice si todo salio bien.
    Write-Host "[2/3] Esperando a que los brokers respondan y se creen los topicos..." -ForegroundColor Cyan
    $terminado = $false
    for ($i = 1; $i -le ($SegundosEspera / 5); $i++) {
        Start-Sleep -Seconds 5
        $estado = (Invoke-Nativo @("docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", "kafka-init")).Trim()
        if ($estado -like "exited *") { $terminado = $true; break }
    }
    if (-not $terminado) { throw "Los topicos no se crearon en $SegundosEspera segundos. Revisa: docker compose logs kafka-init" }
    if ($estado -ne "exited 0") {
        Write-Host (Invoke-Nativo @("docker", "logs", "kafka-init")) -ForegroundColor DarkGray
        throw "kafka-init termino con error ($estado)."
    }

    Write-Host "[3/3] Topicos del caso:" -ForegroundColor Cyan
    foreach ($topico in "pedidos-eventos", "pedidos-auditoria") {
        $descripcion = Invoke-Nativo @("docker", "exec", "kafka-1", "kafka-topics",
            "--bootstrap-server", "kafka-1:9092", "--describe", "--topic", $topico)
        Write-Host ""
        ($descripcion -split "`r?`n") | Where-Object { $_.Trim() } | ForEach-Object { Write-Host "  $_" }
    }

    Write-Host ""
    Write-Host "  Kafka UI: http://localhost:8090" -ForegroundColor Green
    Write-Host "  Brokers desde el equipo: localhost:29092,localhost:39092,localhost:49092" -ForegroundColor Green
    Write-Host "  Brokers dentro de Docker: kafka-1:9092,kafka-2:9092,kafka-3:9092" -ForegroundColor Green
    Write-Host ""
}
finally {
    Pop-Location
}
