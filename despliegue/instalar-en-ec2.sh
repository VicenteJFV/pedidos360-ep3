#!/bin/bash
# Instala o actualiza el despliegue (EP1 + EP2 + EP3) en la EC2. Se ejecuta EN la
# instancia, como root, y lo invoca aws/07-desplegar-ep3.ps1 despues de subir el
# paquete.
#
# Es idempotente: correrlo dos veces deja lo mismo que correrlo una. Las
# credenciales que genera (cookie de Erlang, usuario de RabbitMQ) se crean solo
# la primera vez y se conservan despues, porque cambiarlas con el cluster ya
# formado dejaria a los nodos sin poder hablarse.
#
# Espera encontrar en /opt/pedidos360-ep2:
#   despliegue/docker-compose.yml, despliegue/oracle.env
#   rabbitmq-cluster/rabbitmq-{1,2}/rabbitmq.conf
#   kafka-cluster/crear-topicos.sh
#   ms-pedidos360-*/Dockerfile, pom.xml y src/main (el codigo: la imagen
#   compila con Maven, ver el Dockerfile)
#   wallet/

set -euo pipefail

RAIZ=/opt/pedidos360-ep2
DESPLIEGUE=$RAIZ/despliegue
COMPOSE="docker compose --project-directory $DESPLIEGUE -f $DESPLIEGUE/docker-compose.yml"
UID_APP=10001

paso() { echo; echo "=== $* ==="; }

# ---------------------------------------------------------------------------
paso "Docker"
# ---------------------------------------------------------------------------
if ! command -v docker >/dev/null; then
    dnf install -y -q docker
fi
systemctl enable --now docker >/dev/null 2>&1
usermod -aG docker ec2-user

if ! docker compose version >/dev/null 2>&1; then
    VERSION=v2.29.7
    DESTINO=/usr/local/lib/docker/cli-plugins
    mkdir -p "$DESTINO"
    cd /tmp
    curl -fsSL -o docker-compose "https://github.com/docker/compose/releases/download/${VERSION}/docker-compose-linux-x86_64"
    curl -fsSL -o docker-compose.sha256 "https://github.com/docker/compose/releases/download/${VERSION}/docker-compose-linux-x86_64.sha256"
    echo "$(cut -d' ' -f1 docker-compose.sha256)  docker-compose" | sha256sum -c -
    install -m 755 docker-compose "$DESTINO/docker-compose"
    rm -f docker-compose docker-compose.sha256
fi
docker --version
docker compose version

# ---------------------------------------------------------------------------
paso "Memoria y disco"
# ---------------------------------------------------------------------------
# EP3 suma el cluster Kafka (7 contenedores) y dos microservicios a los 8 de
# EP2. Con 4 GB (t3.medium) el kernel empieza a matar contenedores por falta de
# memoria; por eso se exige una instancia de 8 GB (t3.large) antes de seguir.
MEMORIA_MB=$(free -m | awk '/^Mem:/{print $2}')
if [ "$MEMORIA_MB" -lt 7000 ]; then
    echo "  [FALLA] La instancia tiene ${MEMORIA_MB} MB de memoria y EP3 necesita unos 8 GB."
    echo "          Cambia el tipo a t3.large: .\\07-desplegar-ep3.ps1 -CambiarTipo"
    exit 1
fi
echo "  memoria: ${MEMORIA_MB} MB"

# Si el volumen se amplio desde AWS, la particion y el sistema de archivos
# siguen del tamano anterior hasta que se extienden aqui. Los dos comandos no
# hacen nada si ya ocupan todo el disco.
DISCO_RAIZ=$(findmnt -n -o SOURCE /)
DISCO=$(lsblk -n -o PKNAME "$DISCO_RAIZ" | head -1)
PARTICION=$(echo "$DISCO_RAIZ" | grep -o '[0-9]*$')
if [ -n "$DISCO" ] && [ -n "$PARTICION" ]; then
    growpart "/dev/$DISCO" "$PARTICION" >/dev/null 2>&1 || true
    xfs_growfs / >/dev/null 2>&1 || true
fi
# Lo que ya no usa ningun contenedor: imagenes de despliegues anteriores.
docker image prune -f >/dev/null 2>&1 || true
LIBRE_MB=$(df -m --output=avail / | tail -1 | tr -d ' ')
if [ "$LIBRE_MB" -lt 5000 ]; then
    echo "  [FALLA] Quedan ${LIBRE_MB} MB libres en disco. Las imagenes de Kafka y los dos"
    echo "          microservicios nuevos necesitan unos 5 GB."
    echo "          Amplia el volumen: .\\07-desplegar-ep3.ps1 -DiscoGb 20"
    exit 1
fi
echo "  disco libre: ${LIBRE_MB} MB"

# ---------------------------------------------------------------------------
paso "Servicios de EP1"
# ---------------------------------------------------------------------------
# Catalogo, pedidos y el BFF pasan a contenedores, como exige la guia. Si
# siguieran como servicios systemd ocuparian los puertos 8081, 8082 y 8080 y
# los contenedores no podrian publicarse. Se deshabilitan, no se borran: si
# hiciera falta volver a EP1, basta con systemctl enable --now.
for unidad in ms-catalog ms-orders bff; do
    if systemctl list-unit-files "$unidad.service" >/dev/null 2>&1; then
        systemctl disable --now "$unidad" >/dev/null 2>&1 || true
        echo "  $unidad detenido y deshabilitado"
    fi
done

# ---------------------------------------------------------------------------
paso "Credenciales"
# ---------------------------------------------------------------------------
test -s "$DESPLIEGUE/oracle.env" || { echo "Falta $DESPLIEGUE/oracle.env"; exit 1; }
chmod 600 "$DESPLIEGUE/oracle.env"

# Se generan una sola vez. Hexadecimal para que no haya caracteres que el
# parser de Compose o el de rabbitmq.conf puedan interpretar.
if [ ! -s "$DESPLIEGUE/.env" ]; then
    {
        echo "RABBITMQ_COOKIE=$(openssl rand -hex 24)"
        echo "RABBITMQ_USER=pedidos360"
        echo "RABBITMQ_PASS=$(openssl rand -hex 16)"
    } > "$DESPLIEGUE/.env"
    echo "  credenciales de RabbitMQ generadas"
else
    echo "  credenciales de RabbitMQ conservadas"
fi
# EP3: usuario de Kafka UI. Se agrega aparte porque en una EC2 que viene de EP2
# el .env ya existe, con la cookie y el usuario de RabbitMQ, y no se regenera.
if ! grep -q '^KAFKAUI_PASS=' "$DESPLIEGUE/.env"; then
    {
        echo "KAFKAUI_USER=pedidos360"
        echo "KAFKAUI_PASS=$(openssl rand -hex 16)"
    } >> "$DESPLIEGUE/.env"
    echo "  credenciales de Kafka UI generadas"
else
    echo "  credenciales de Kafka UI conservadas"
fi
chmod 600 "$DESPLIEGUE/.env"

# El usuario por defecto del broker se fija en un archivo de configuracion y no
# con variables de entorno: RabbitMQ 4.x ignora varias de las variables que
# usaban las versiones anteriores. Con default_user definido, el broker ya no
# crea guest/guest.
. "$DESPLIEGUE/.env"
cat > "$DESPLIEGUE/rabbitmq-usuario.conf" <<EOF
default_user = $RABBITMQ_USER
default_pass = $RABBITMQ_PASS
EOF
# Solo lo lee el usuario con el que corre RabbitMQ dentro del contenedor. Su UID
# se le pregunta a la imagen en vez de suponerlo.
IMAGEN_RABBIT=rabbitmq:4.2.9-management
docker pull -q "$IMAGEN_RABBIT" >/dev/null
UID_RABBIT=$(docker run --rm --entrypoint id "$IMAGEN_RABBIT" -u rabbitmq)
GID_RABBIT=$(docker run --rm --entrypoint id "$IMAGEN_RABBIT" -g rabbitmq)
chown "$UID_RABBIT:$GID_RABBIT" "$DESPLIEGUE/rabbitmq-usuario.conf"
chmod 600 "$DESPLIEGUE/rabbitmq-usuario.conf"
echo "  usuario de RabbitMQ: $RABBITMQ_USER (sin guest/guest)"

# El wallet es una credencial: cwallet.sso abre la base sin contrasena. Solo lo
# puede leer el usuario con el que corren los microservicios en el contenedor.
test -s "$RAIZ/wallet/cwallet.sso" || { echo "Falta el wallet en $RAIZ/wallet"; exit 1; }
chown -R "$UID_APP:$UID_APP" "$RAIZ/wallet"
chmod 700 "$RAIZ/wallet"
chmod 600 "$RAIZ/wallet"/*

# ---------------------------------------------------------------------------
paso "Imagenes"
# ---------------------------------------------------------------------------
# Maven y Node compilan dentro de las imagenes. La primera vez descargan las
# dependencias y tardan unos minutos cada una; despues las toman del cache de
# BuildKit.
construir() {
    echo "  construyendo pedidos360/$1:ep2..."
    docker build -q -t "pedidos360/$1:ep2" "$2" >/dev/null
    echo "  pedidos360/$1:ep2 lista"
}
for m in catalog orders notify rabbitadmin reportes kafkaadmin; do
    construir "$m" "$RAIZ/ms-pedidos360-$m"
done
construir bff "$RAIZ/bff"
construir frontend "$RAIZ/frontend"

# ---------------------------------------------------------------------------
paso "Contenedores"
# ---------------------------------------------------------------------------
# Marca de tiempo para revisar solo los logs de este despliegue: un contenedor
# que no se recrea conserva los de antes.
INICIO=$(date -u +%Y-%m-%dT%H:%M:%SZ)
$COMPOSE up -d --remove-orphans

# Defensa ante un montaje viejo del wallet. Un contenedor que no se recrea en
# este despliegue sigue montando la carpeta que existia cuando arranco; si
# alguien la borro y la volvio a crear, el contenedor la ve vacia y falla al
# abrir conexiones nuevas a Oracle. Reiniciarlo vuelve a montar la actual.
for servicio in catalog orders reportes; do
    if ! docker exec "ms-pedidos360-$servicio" test -f /app/wallet/tnsnames.ora 2>/dev/null; then
        $COMPOSE restart "$servicio" >/dev/null 2>&1
        echo "  $servicio reiniciado: no veia el wallet actual"
    fi
done

# Comando corto para la demostracion: los logs de pedidos (productor) y notify
# (consumidor) en vivo. Pensado para la terminal de Session Manager en la
# consola de AWS, que se abre desde el navegador sin llave SSH: ahi escribir el
# docker compose completo seria lento y facil de equivocar. Se deja el prefijo
# con el nombre del contenedor para distinguir que linea es de cada uno.
cat > /usr/local/bin/logs-demo <<EOF
#!/bin/bash
# Logs en vivo de orders (productor), notify (consumidor RabbitMQ) y reportes
# (consumidor Kafka). Ctrl+C para salir. Con argumentos, solo esos servicios:
#   logs-demo orders reportes
SERVICIOS="\${*:-orders notify reportes}"
exec sudo docker compose --project-directory $DESPLIEGUE -f $DESPLIEGUE/docker-compose.yml logs -f --tail 15 \$SERVICIOS
EOF
chmod 755 /usr/local/bin/logs-demo

# El agente de Session Manager viene en Amazon Linux 2023. Si el perfil de
# instancia se acaba de asignar, el agente tarda en notarlo; reiniciarlo hace
# que se registre de inmediato y la terminal quede disponible en la consola.
if systemctl list-unit-files amazon-ssm-agent.service >/dev/null 2>&1; then
    systemctl restart amazon-ssm-agent >/dev/null 2>&1 || true
fi

# ---------------------------------------------------------------------------
paso "Cluster RabbitMQ"
# ---------------------------------------------------------------------------
for nodo in rabbitmq-1 rabbitmq-2; do
    for _ in $(seq 1 30); do
        docker exec "$nodo" rabbitmq-diagnostics -q ping >/dev/null 2>&1 && break
        sleep 5
    done
done

# Igual que levantar-cluster.ps1: solo se une si todavia no esta unido. La
# membresia sobrevive en el volumen, asi que en los despliegues siguientes este
# paso no hace nada.
# Se guarda la salida antes de buscar en ella: con pipefail, un grep -q que
# corta la tuberia puede hacer fallar al comando anterior y dar un falso "no
# esta unido", y unir un nodo ya unido aborta el script.
estado=$(docker exec rabbitmq-1 rabbitmqctl cluster_status 2>/dev/null || true)
if grep -q "rabbit@rabbitmq-2" <<<"$estado"; then
    echo "  el cluster ya estaba formado"
else
    docker exec rabbitmq-2 rabbitmqctl stop_app >/dev/null
    docker exec rabbitmq-2 rabbitmqctl join_cluster rabbit@rabbitmq-1 >/dev/null
    docker exec rabbitmq-2 rabbitmqctl start_app >/dev/null
    echo "  rabbitmq-2 unido al cluster de rabbitmq-1"
fi
echo "  nodos corriendo:"
docker exec rabbitmq-1 rabbitmqctl cluster_status 2>/dev/null |
    awk '/^Running Nodes/{f=1;next} /^[A-Z]/{f=0} f && /^rabbit@/{print "    " $0}'

# ---------------------------------------------------------------------------
paso "Cluster Kafka"
# ---------------------------------------------------------------------------
# kafka-init espera a los tres brokers, crea los topicos y termina: su codigo de
# salida dice si el cluster quedo listo.
for _ in $(seq 1 60); do
    estado_init=$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' kafka-init 2>/dev/null || true)
    case "$estado_init" in exited*) break ;; esac
    sleep 5
done
if [ "$estado_init" != "exited 0" ]; then
    echo "  [FALLA] Los topicos no se crearon (kafka-init: ${estado_init:-sin estado})."
    docker logs --tail 20 kafka-init 2>&1 | sed 's/^/    /'
    exit 1
fi
# Brokers registrados en ZooKeeper: deben ser 1, 2 y 3.
echo "  brokers: $(docker exec zookeeper-1 zookeeper-shell localhost:2181 ls /brokers/ids 2>/dev/null | tail -1 || true)"
descripcion=$(docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe 2>/dev/null || true)
for topico in pedidos-eventos pedidos-auditoria; do
    grep "^Topic: $topico[[:space:]]" <<<"$descripcion" |
        sed -E 's/TopicId: [^[:space:]]+[[:space:]]+//; s/Configs:.*//; s/^/  /' || true
done

# ---------------------------------------------------------------------------
paso "Salud de los servicios"
# ---------------------------------------------------------------------------
# Que cuenta como "responde" depende del servicio:
#   microservicios  /actuator/health en 200
#   bff             401 en /api/catalog: esta vivo y exige el JWT, que es
#                   justo lo correcto. No tiene actuator.
#   frontend        200 en /app/, sirviendo el index.html con su base href
responde() {
    case "$1" in
        bff)
            [ "$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:8080/api/catalog)" = "401" ] ;;
        kafka-ui)
            # Cualquier respuesta HTTP sirve: sin sesion, redirige al login.
            [ "$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://127.0.0.1:8090/)" != "000" ] ;;
        frontend)
            curl -fs -m 5 http://127.0.0.1/app/ | grep '<base href="/Desarrollo/app/">' >/dev/null ;;
        *)
            curl -fs -m 5 "http://127.0.0.1:$1/actuator/health" >/dev/null ;;
    esac
}

# Pedidos y catalogo tardan mas: en el primer arranque Hibernate crea las tablas
# en Oracle Cloud, y la base esta en Santiago.
#
# Si la contrasena de Oracle esta mal, catalogo y pedidos fallan al arrancar y
# restart: unless-stopped los vuelve a lanzar sin fin. Cada arranque es un login
# fallido, y ADMIN se bloquea tras unos pocos: se comprobo en una prueba con un
# usuario inexistente, que acumulo 75 intentos en minutos. Por eso, apenas
# aparece ORA-01017 se detienen los dos, en vez de esperar a que agoten el
# tiempo de espera.
pendientes="8082 8081 8083 8084 8085 8086 bff frontend kafka-ui"
for _ in $(seq 1 60); do
    for c in ms-pedidos360-catalog ms-pedidos360-orders ms-pedidos360-reportes; do
        # Sin -q: grep -q corta la tuberia al primer hallazgo, docker logs recibe
        # SIGPIPE y con pipefail la tuberia entera da falso justo cuando SI
        # encontro el error. Leyendo todo, la condicion es fiable.
        if docker logs --since "$INICIO" "$c" 2>&1 | grep -E "ORA-01017|ORA-28000" >/dev/null; then
            $COMPOSE stop catalog orders reportes >/dev/null 2>&1
            echo
            echo "  [FALLA] Oracle rechazo el usuario o la contrasena ($c)."
            echo "          Se detuvieron catalogo y pedidos para no bloquear la cuenta ADMIN."
            echo "          Revisa la contrasena y vuelve a desplegar."
            exit 1
        fi
    done
    siguen=""
    for servicio in $pendientes; do
        responde "$servicio" || siguen="$siguen $servicio"
    done
    pendientes=$siguen
    [ -z "$pendientes" ] && break
    sleep 5
done

for par in 8082:catalog:ms-pedidos360-catalog 8081:orders:ms-pedidos360-orders \
           8083:notify:ms-pedidos360-notify 8084:rabbitadmin:ms-pedidos360-rabbitadmin \
           8085:reportes:ms-pedidos360-reportes 8086:kafkaadmin:ms-pedidos360-kafkaadmin \
           kafka-ui:kafka-ui:kafka-ui \
           bff:bff:bff-pedidos360 frontend:frontend:frontend-pedidos360; do
    clave=${par%%:*}; resto=${par#*:}; nombre=${resto%%:*}; contenedor=${resto##*:}
    if responde "$clave"; then
        echo "  [OK]    $nombre"
    else
        echo "  [FALLA] $nombre: docker logs $contenedor"
    fi
done

if [ -n "$pendientes" ]; then
    exit 1
fi

echo
echo "Memoria del host:"
free -m | head -2
