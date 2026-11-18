<#
.SINOPSIS
    Prueba completa de Kafka en EP3: cluster, topicos, flujo de eventos,
    errores y DLT, administrador Kafka, consumer groups y continuidad con EP2.

.DESCRIPCION
    Recorre lo que piden la guia de EP3 (secciones 4 a 7 y 10), la guia 3.4
    (cluster, lideres, replicas e ISR, caida de un broker) y la guia 3.4.1 (las
    8 pruebas del administrador). No levanta nada: prueba lo que ya esta
    corriendo.

    Dos modos:

      Local  (por defecto) contra el ensayo de despliegue en Docker Desktop:
             despliegue\probar-en-local.ps1. Cada microservicio en su puerto,
             sin token.

      Nube   contra la EC2, todo por el API Gateway con el JWT de un usuario
             Admin (obtener-token.ps1 -Cuenta). Sirve desde cualquier PC.

    -PruebaCaida (solo local) detiene kafka-2, comprueba que el ISR baja a dos
    replicas y que un pedido igual se publica y se consume, y lo recupera.

.EJEMPLO
    .\verificar-ep3.ps1

.EJEMPLO
    .\verificar-ep3.ps1 -PruebaCaida

.EJEMPLO
    $t = .\obtener-token.ps1 -Cuenta "Admin@luissantacruz2026.onmicrosoft.com"
    .\verificar-ep3.ps1 -BaseUrl "https://xpglku1pj5.execute-api.us-east-1.amazonaws.com/Desarrollo" -Token (Get-Clipboard)
#>
[CmdletBinding()]
param(
    [string]$BaseUrl = "",
    [string]$Token = "",
    [switch]$PruebaCaida
)

$ErrorActionPreference = "Stop"
$script:ok = 0
$script:fallos = @()

$nube = [bool]$BaseUrl
if ($nube) {
    if (-not $Token) { throw "En modo nube hace falta -Token con el JWT de un usuario Admin." }
    $BaseUrl = $BaseUrl.TrimEnd("/")
    $urlCat = $BaseUrl; $urlPed = $BaseUrl; $urlRabbit = $BaseUrl; $urlReportes = $BaseUrl; $urlKafka = $BaseUrl
    if ($PruebaCaida) { throw "-PruebaCaida detiene un contenedor: solo esta disponible en local." }
} else {
    $urlCat = "http://127.0.0.1:8082"; $urlPed = "http://127.0.0.1:8081"; $urlRabbit = "http://127.0.0.1:8084"
    $urlReportes = "http://127.0.0.1:8085"; $urlKafka = "http://127.0.0.1:8086"
}

# ---------------------------------------------------------------------------
# Utilidades (las mismas de verificar-ep2.ps1)
# ---------------------------------------------------------------------------

function Invoke-Api {
    param([string]$Metodo, [string]$Url, $Cuerpo)
    $params = @{ Method = $Metodo; Uri = $Url; UseBasicParsing = $true; TimeoutSec = 30; Headers = @{} }
    if ($Token) { $params.Headers["Authorization"] = "Bearer $Token" }
    if ($null -ne $Cuerpo) {
        $params.Body = ($Cuerpo | ConvertTo-Json -Depth 6)
        $params.ContentType = "application/json; charset=utf-8"
    }
    try {
        $r = Invoke-WebRequest @params
        $json = $null
        if ($r.Content) { try { $json = $r.Content | ConvertFrom-Json } catch { } }
        return @{ Status = [int]$r.StatusCode; Json = $json; Texto = [string]$r.Content }
    } catch {
        $resp = $_.Exception.Response
        if (-not $resp) { return @{ Status = 0; Json = $null; Texto = $_.Exception.Message } }
        # En PowerShell 5.1 el cuerpo de un error queda en ErrorDetails.Message.
        $texto = ""
        if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $texto = $_.ErrorDetails.Message }
        $json = $null
        if ($texto) { try { $json = $texto | ConvertFrom-Json } catch { } }
        return @{ Status = [int]$resp.StatusCode; Json = $json; Texto = $texto }
    }
}

function Comprobar {
    param([string]$Nombre, [bool]$Condicion, [string]$Detalle = "")
    if ($Condicion) {
        Write-Host ("  [OK]    " + $Nombre) -ForegroundColor Green
        $script:ok++
    } else {
        Write-Host ("  [FALLA] " + $Nombre) -ForegroundColor Red
        if ($Detalle) { Write-Host ("          " + $Detalle) -ForegroundColor DarkGray }
        $script:fallos += $Nombre
    }
}

function Seccion([string]$Texto) {
    Write-Host ""
    Write-Host "=== $Texto ===" -ForegroundColor Cyan
}

<# El consumo es asincrono: se reintenta la condicion hasta que se cumple o se agota el tiempo. #>
function Esperar {
    param([scriptblock]$Condicion, [int]$Segundos = 30)
    for ($i = 0; $i -lt $Segundos; $i++) {
        try { if (& $Condicion) { return $true } } catch { }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Crear-Pedido([string]$Email) {
    return Invoke-Api POST "$urlPed/api/orders" @{
        clienteEmail = $Email; clienteNombre = "Prueba EP3"; direccionEntrega = "Av. Siempre Viva 742"
        items = @(@{ productoId = $script:productoId; cantidad = 1 })
    }
}

function Auditoria([string]$Codigo) {
    $r = Invoke-Api GET "$urlReportes/api/reportes/auditoria?pedido=$Codigo"
    if ($r.Status -ne 200) { return @() }
    return @($r.Json)
}

function Mensajes-Topico([string]$Topico) {
    $r = Invoke-Api GET "$urlKafka/api/kafka/topics/$Topico"
    if ($r.Status -ne 200) { return -1 }
    return [long]$r.Json.messages
}

Write-Host ""
Write-Host "=== Verificacion de EP3 - Kafka ===" -ForegroundColor Cyan
Write-Host ("  Modo: " + $(if ($nube) { "nube, por el API Gateway ($BaseUrl)" } else { "local (Docker Desktop)" }))

# ---------------------------------------------------------------------------
Seccion "1. Cluster: tres brokers (guia EP3 seccion 3, guia 3.4)"
# ---------------------------------------------------------------------------
$r = Invoke-Api GET "$urlKafka/api/kafka/cluster"
Comprobar "El administrador Kafka responde" ($r.Status -eq 200) "status=$($r.Status) $($r.Texto)"
if ($r.Status -eq 401 -or $r.Status -eq 403) {
    Write-Host "  El token no sirve o no es de un Admin: corre obtener-token.ps1 -Cuenta con la cuenta Admin." -ForegroundColor Yellow
    exit 1
}
$brokers = @($r.Json.brokers | ForEach-Object { $_.id } | Sort-Object)
Comprobar "Hay tres brokers activos (1, 2 y 3)" (($brokers -join ",") -eq "1,2,3") "brokers=$($brokers -join ',')"
Comprobar "El cluster tiene un controlador" ($null -ne $r.Json.controller)

# ---------------------------------------------------------------------------
Seccion "2. Topicos del caso: 3 particiones, factor 3 e ISR completo (seccion 4)"
# ---------------------------------------------------------------------------
$retencionEsperada = @{ "pedidos-eventos" = "604800000"; "pedidos-auditoria" = "2592000000" }
foreach ($topico in "pedidos-eventos", "pedidos-auditoria") {
    $d = Invoke-Api GET "$urlKafka/api/kafka/topics/$topico"
    Comprobar "$topico existe" ($d.Status -eq 200) "status=$($d.Status)"
    if ($d.Status -ne 200) { continue }
    $parts = @($d.Json.partitions)
    Comprobar "$topico tiene 3 particiones" ($parts.Count -eq 3) "particiones=$($parts.Count)"
    Comprobar "$topico tiene factor de replicacion 3" ($d.Json.replicationFactor -eq 3) "factor=$($d.Json.replicationFactor)"
    Comprobar "$topico tiene el ISR completo en todas las particiones" ($d.Json.underReplicated -eq 0) "underReplicated=$($d.Json.underReplicated)"
    $lideres = @($parts | ForEach-Object { $_.leader } | Sort-Object -Unique)
    Comprobar "$topico reparte los lideres entre los brokers" ($lideres.Count -ge 2) "lideres=$($lideres -join ',')"

    $c = Invoke-Api GET "$urlKafka/api/kafka/topics/$topico/configs"
    Comprobar "$topico retention.ms = $($retencionEsperada[$topico])" ($c.Json.'retention.ms' -eq $retencionEsperada[$topico]) "retention.ms=$($c.Json.'retention.ms')"
    Comprobar "$topico cleanup.policy = delete" ($c.Json.'cleanup.policy' -eq "delete") "cleanup.policy=$($c.Json.'cleanup.policy')"
    Comprobar "$topico min.insync.replicas = 2" ($c.Json.'min.insync.replicas' -eq "2") "min.insync.replicas=$($c.Json.'min.insync.replicas')"
}

# ---------------------------------------------------------------------------
Seccion "3. Flujo: el productor unico publica en los dos topicos y el consumidor procesa (seccion 5)"
# ---------------------------------------------------------------------------
$cat = Invoke-Api GET "$urlCat/api/catalog"
$producto = @($cat.Json | Where-Object { $_.activo -and $_.stock -gt 10 })[0]
Comprobar "Hay un producto con stock para las pruebas" ($null -ne $producto) "status=$($cat.Status)"
$script:productoId = $producto.id

$p = Crear-Pedido "cliente@duoc.cl"
Comprobar "Se crea un pedido" ($p.Status -eq 201) "status=$($p.Status) $($p.Texto)"
$codigo = $p.Json.codigo
foreach ($accion in "accept", "prepare", "ship", "deliver") {
    $a = Invoke-Api PUT "$urlPed/api/orders/$($p.Json.id)/$accion"
    Comprobar "PUT $accion responde 200" ($a.Status -eq 200) "status=$($a.Status) $($a.Texto)"
}

$llego = Esperar { @(Auditoria $codigo).Count -ge 5 }
$historia = @(Auditoria $codigo)
Comprobar "La auditoria del pedido tiene las 5 acciones (pedidos-auditoria)" $llego "acciones=$((@($historia) | ForEach-Object { $_.accion }) -join ',')"
Comprobar "Las acciones llegan en orden" ((($historia | ForEach-Object { $_.accion }) -join ",") -eq "CREAR,ACEPTAR,PREPARAR,DESPACHAR,ENTREGAR")
$particiones = @($historia | ForEach-Object { $_.particion } | Sort-Object -Unique)
Comprobar "Todos los eventos del pedido van a la misma particion (key = codigo)" ($particiones.Count -eq 1) "particiones=$($particiones -join ',')"
$offsets = @($historia | ForEach-Object { [long]$_.offset })
$crecientes = $offsets.Count -ge 2; for ($i = 1; $i -lt $offsets.Count; $i++) { if ($offsets[$i] -le $offsets[$i - 1]) { $crecientes = $false } }
Comprobar "Sus offsets son crecientes" $crecientes "offsets=$($offsets -join ',')"
if ($nube) {
    $usuario = if ($historia.Count -gt 0) { $historia[0].usuario } else { $null }
    Comprobar "La auditoria registra al usuario del token, no 'sistema'" ($usuario -and $usuario -ne "sistema") "usuario=$usuario"
}

$reporte = $null
$enReporte = Esperar {
    $script:reporte = @((Invoke-Api GET "$urlReportes/api/reportes/pedidos?estado=ENTREGADO").Json) | Where-Object { $_.codigo -eq $codigo }
    $null -ne $script:reporte -and $script:reporte.eventosRecibidos -ge 5
}
Comprobar "El reporte (pedidos-eventos) muestra el pedido ENTREGADO con sus 5 eventos" $enReporte "reporte=$($script:reporte | ConvertTo-Json -Compress)"

$res = Invoke-Api GET "$urlReportes/api/reportes/pedidos/resumen"
Comprobar "El resumen de la reporteria responde" ($res.Status -eq 200 -and $res.Json.totalPedidos -ge 1) "status=$($res.Status)"

# ---------------------------------------------------------------------------
Seccion "4. Errores: reintentos y DLT (seccion 7)"
# ---------------------------------------------------------------------------
$antes = Mensajes-Topico "pedidos-eventos-dlt"
Comprobar "El DLT pedidos-eventos-dlt existe" ($antes -ge 0)
$f = Crear-Pedido "error-kafka@pedidos360.cl"
Comprobar "El pedido con error-kafka@ se crea igual (el negocio no depende del consumidor)" ($f.Status -eq 201) "status=$($f.Status)"
# 3 intentos con 1 s entre ellos: unos 3 segundos hasta el DLT
$alDlt = Esperar { (Mensajes-Topico "pedidos-eventos-dlt") -gt $antes } 30
Comprobar "Tras agotar los reintentos, el evento llega a pedidos-eventos-dlt" $alDlt "antes=$antes despues=$(Mensajes-Topico 'pedidos-eventos-dlt')"
$fallido = @((Invoke-Api GET "$urlReportes/api/reportes/pedidos").Json) | Where-Object { $_.codigo -eq $f.Json.codigo }
Comprobar "El evento fallido no quedo a medias en el reporte" ($null -eq $fallido)

$g = Invoke-Api GET "$urlKafka/api/kafka/consumer-groups/reportes-pedidos"
Comprobar "El consumidor sigue activo despues del error (grupo STABLE)" ($g.Json.state -eq "STABLE") "state=$($g.Json.state)"

# ---------------------------------------------------------------------------
Seccion "5. Administrador Kafka: las 8 pruebas de la guia 3.4.1 (seccion 6)"
# ---------------------------------------------------------------------------
$prueba = "ev3-admin-prueba"
Invoke-Api DELETE "$urlKafka/api/kafka/topics/$prueba" | Out-Null   # restos de una corrida anterior
Start-Sleep -Seconds 2

$t = Invoke-Api GET "$urlKafka/api/kafka/topics"
Comprobar "P1 Listar: incluye los dos topicos del caso" (@($t.Json) -contains "pedidos-eventos" -and @($t.Json) -contains "pedidos-auditoria") "topicos=$(@($t.Json) -join ',')"

$c = Invoke-Api POST "$urlKafka/api/kafka/topics" @{ name = $prueba; partitions = 3; replicationFactor = 3; configs = @{ "retention.ms" = "86400000"; "cleanup.policy" = "delete" } }
Comprobar "P2 Crear ${prueba}: 201" ($c.Status -eq 201) "status=$($c.Status) $($c.Texto)"

$d = Invoke-Api GET "$urlKafka/api/kafka/topics/$prueba"
Comprobar "P3 Describir: 3 particiones con 3 replicas e ISR completo" (@($d.Json.partitions).Count -eq 3 -and $d.Json.replicationFactor -eq 3 -and $d.Json.underReplicated -eq 0) "$($d.Texto)"

$cf = Invoke-Api GET "$urlKafka/api/kafka/topics/$prueba/configs"
Comprobar "P4 Configuracion: retention.ms=86400000 y cleanup.policy=delete" ($cf.Json.'retention.ms' -eq "86400000" -and $cf.Json.'cleanup.policy' -eq "delete")

$m = Invoke-Api PATCH "$urlKafka/api/kafka/topics/$prueba/configs" @{ configName = "retention.ms"; value = "172800000" }
$cf = Invoke-Api GET "$urlKafka/api/kafka/topics/$prueba/configs"
Comprobar "P5 Modificar retention.ms a 172800000 (48 h) y comprobarlo" ($m.Status -eq 200 -and $cf.Json.'retention.ms' -eq "172800000") "status=$($m.Status) retention.ms=$($cf.Json.'retention.ms')"

$pa = Invoke-Api PATCH "$urlKafka/api/kafka/topics/$prueba/partitions" @{ totalPartitions = 4 }
$cuatro = Esperar { @((Invoke-Api GET "$urlKafka/api/kafka/topics/$prueba").Json.partitions).Count -eq 4 } 15
Comprobar "P6 Aumentar a 4 particiones y comprobarlo" ($pa.Status -eq 200 -and $cuatro) "status=$($pa.Status)"

$gr = Invoke-Api GET "$urlKafka/api/kafka/consumer-groups"
$ids = @($gr.Json | ForEach-Object { $_.groupId })
Comprobar "P7 Consumer groups: reportes-pedidos y reportes-auditoria" ($ids -contains "reportes-pedidos" -and $ids -contains "reportes-auditoria") "grupos=$($ids -join ',')"

$del = Invoke-Api DELETE "$urlKafka/api/kafka/topics/$prueba"
$fuera = Esperar { @((Invoke-Api GET "$urlKafka/api/kafka/topics").Json) -notcontains $prueba } 15
Comprobar "P8 Eliminar el topico de prueba y comprobar que ya no esta" ($del.Status -eq 200 -and $fuera) "status=$($del.Status)"

# ---------------------------------------------------------------------------
Seccion "6. Consumer groups: offsets, lag y distribucion (seccion 6.2)"
# ---------------------------------------------------------------------------
foreach ($grupo in "reportes-pedidos", "reportes-auditoria") {
    $g = Invoke-Api GET "$urlKafka/api/kafka/consumer-groups/$grupo"
    Comprobar "$grupo esta STABLE" ($g.Json.state -eq "STABLE") "state=$($g.Json.state)"
    $miembros = @($g.Json.members)
    Comprobar "$grupo reparte las 3 particiones entre 3 miembros" ($miembros.Count -eq 3 -and @($miembros | Where-Object { @($_.partitions).Count -eq 1 }).Count -eq 3) "miembros=$($miembros.Count)"
    $alDia = Esperar { (Invoke-Api GET "$urlKafka/api/kafka/consumer-groups/$grupo").Json.totalLag -eq 0 } 20
    Comprobar "$grupo termina con lag 0 (proceso todo lo publicado)" $alDia "lag=$((Invoke-Api GET "$urlKafka/api/kafka/consumer-groups/$grupo").Json.totalLag)"
}

# ---------------------------------------------------------------------------
Seccion "7. Validacion y protecciones (400, 404, 409)"
# ---------------------------------------------------------------------------
$v = Invoke-Api POST "$urlKafka/api/kafka/topics" @{ name = ""; partitions = 3; replicationFactor = 3 }
Comprobar "Topico sin nombre: 400 que nombra el campo" ($v.Status -eq 400 -and $v.Json.parametro -eq "name") "status=$($v.Status) $($v.Texto)"
$v = Invoke-Api POST "$urlKafka/api/kafka/topics" @{ name = "t-rf"; partitions = 3; replicationFactor = 4 }
Comprobar "Factor de replicacion mayor que los brokers: 400" ($v.Status -eq 400 -and $v.Json.parametro -eq "replicationFactor") "status=$($v.Status)"
$v = Invoke-Api PATCH "$urlKafka/api/kafka/topics/pedidos-eventos/configs" @{ configName = "retention.ms"; value = "-5" }
Comprobar "retention.ms fuera de rango: 400" ($v.Status -eq 400) "status=$($v.Status)"
$v = Invoke-Api DELETE "$urlKafka/api/kafka/topics/pedidos-eventos"
Comprobar "Eliminar un topico del caso: 409" ($v.Status -eq 409) "status=$($v.Status)"
$v = Invoke-Api DELETE "$urlKafka/api/kafka/consumer-groups/reportes-pedidos"
Comprobar "Eliminar un grupo del caso: 409" ($v.Status -eq 409) "status=$($v.Status)"
$v = Invoke-Api GET "$urlKafka/api/kafka/topics/no-existe-ep3"
Comprobar "Consultar un topico que no existe: 404" ($v.Status -eq 404) "status=$($v.Status)"
$v = Invoke-Api GET "$urlReportes/api/reportes/pedidos?estado=PERDIDO"
Comprobar "Reporte con un estado que no existe: 400" ($v.Status -eq 400) "status=$($v.Status)"

# ---------------------------------------------------------------------------
Seccion "8. Continuidad: RabbitMQ y EP1 siguen operativos (seccion 8)"
# ---------------------------------------------------------------------------
$rb = Invoke-Api GET "$urlRabbit/api/rabbitmq/metricas"
Comprobar "La API de RabbitMQ (EP2) responde con sus 6 colas" ($rb.Status -eq 200 -and $rb.Json.colasActivas -eq 6) "status=$($rb.Status) colas=$($rb.Json.colasActivas)"
Comprobar "Los consumidores de RabbitMQ siguen conectados" ($rb.Json.consumidores -ge 3) "consumidores=$($rb.Json.consumidores)"
$o = Invoke-Api GET "$urlPed/api/orders"
Comprobar "La API de pedidos (EP1) responde" ($o.Status -eq 200) "status=$($o.Status)"

# ---------------------------------------------------------------------------
if ($PruebaCaida) {
    Seccion "9. Caida y recuperacion de un broker (guia 3.4, etapa 8)"
    docker stop kafka-2 | Out-Null
    $bajo = Esperar { (Invoke-Api GET "$urlKafka/api/kafka/topics/pedidos-eventos").Json.underReplicated -eq 3 } 60
    $d = Invoke-Api GET "$urlKafka/api/kafka/topics/pedidos-eventos"
    Comprobar "Con kafka-2 detenido, el ISR baja a dos replicas en las 3 particiones" $bajo "isr=$((@($d.Json.partitions) | ForEach-Object { $_.isr -join '+' }) -join ' ')"
    Comprobar "Todas las particiones siguen con lider" (@($d.Json.partitions | Where-Object { $null -eq $_.leader }).Count -eq 0)

    # Hasta 90 s: si el coordinador de los consumer groups estaba en kafka-2,
    # los consumidores tienen que descubrir uno nuevo y volver a unirse al
    # grupo antes de seguir leyendo. La publicacion, en cambio, es inmediata.
    $p = Crear-Pedido "cliente@duoc.cl"
    $sigue = $p.Status -eq 201 -and (Esperar { @(Auditoria $p.Json.codigo).Count -ge 1 } 90)
    Comprobar "Con un broker caido, un pedido igual se publica y se consume (acks=all, min.insync.replicas=2)" $sigue

    docker start kafka-2 | Out-Null
    $recuperado = Esperar { (Invoke-Api GET "$urlKafka/api/kafka/topics/pedidos-eventos").Json.underReplicated -eq 0 } 120
    Comprobar "Al volver kafka-2, el ISR vuelve a tener las tres replicas" $recuperado
}

# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "===================================" -ForegroundColor Cyan
$color = if ($script:fallos.Count -eq 0) { "Green" } else { "Red" }
Write-Host (" {0} correctas, {1} fallidas" -f $script:ok, $script:fallos.Count) -ForegroundColor $color
foreach ($f in $script:fallos) { Write-Host "   - $f" -ForegroundColor Red }
Write-Host "===================================" -ForegroundColor Cyan
if ($script:fallos.Count -gt 0) { exit 1 }
