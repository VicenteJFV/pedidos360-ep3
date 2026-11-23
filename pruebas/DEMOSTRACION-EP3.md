# Guion de la presentación — EP3 Kafka (Caso 0)

Diez minutos, en el orden de la sección 10.1 de la guía de EP3. Todo corre en la EC2: se presenta desde el navegador y Postman de un PC del instituto, sin llave SSH ni túnel.

## Antes de entrar (30 a 60 minutos antes)

1. **Oracle:** en la consola de OCI, la base `pedidos360` debe decir *Available*.
2. **Lab:** AWS Academy → *Start Lab*. La EC2 (t3.large) arranca sola y los 17 contenedores vuelven solos. **Espera unos 5 minutos**: Kafka, catálogo, pedidos y reportes tardan en estar listos.
3. **Dos terminales de Session Manager:** EC2 → Instancias → `pedidos360-bff` → Conectar → Session Manager → Conectar. Abre dos pestañas.
   - Terminal 1, para los comandos de este guion.
   - Terminal 2, con los logs en vivo de orders (productor), notify (consumidor RabbitMQ) y reportes (consumidor Kafka):
     ```
     logs-demo
     ```
4. **Contraseñas de Kafka UI y RabbitMQ** (usuario `pedidos360` en ambos), en la Terminal 1:
   ```
   sudo grep -E 'KAFKAUI_PASS|RABBITMQ_PASS' /opt/pedidos360-ep2/despliegue/.env
   ```
5. **Pestañas del navegador:**
   - Aplicación: `https://xpglku1pj5.execute-api.us-east-1.amazonaws.com/Desarrollo/app/` (sesión de Admin), en **dos pestañas**: una en **Catálogo / Panel Admin** y otra en **Kafka**.
   - Kafka UI: `http://32.194.66.142:8090`
   - RabbitMQ Management: `http://32.194.66.142:15672`
6. **Postman:** colección *Pedidos360 EP3 - Kafka* con el environment *Pedidos360 EP3 - EC2* y un token de Admin recién obtenido (dura una hora):
   ```powershell
   cd C:\dev\pedidos360-ep3\pruebas ; .\obtener-token.ps1 -Cuenta "Admin@luissantacruz2026.onmicrosoft.com"
   ```
   Pega el token en la variable `token`. Corre una vez la carpeta **0. Preparacion**.

---

## 0:00 – 1:00 · El caso y el rol de Kafka

> «Pedidos360 ya tenía RabbitMQ para las órdenes de trabajo: enviar el correo, imprimir el ticket de cocina, generar la boleta. Esos mensajes se consumen y desaparecen. Kafka se agrega para algo distinto: **conservar el historial de hechos** del pedido —se creó, se aceptó, se despachó— para reportería y auditoría. El único productor es `ms-pedidos360-orders`, porque es el microservicio que crea los pedidos y cambia su estado. El consumidor es `ms-pedidos360-reportes`.»

## 1:00 – 2:00 · Docker Compose: tres ZooKeeper y tres brokers

Terminal 1:

```
sudo docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "zookeeper|kafka"
```

Deben aparecer `zookeeper-1`, `zookeeper-2`, `zookeeper-3`, `kafka-1`, `kafka-2`, `kafka-3` y `kafka-ui`, todos *Up*.

> «ZooKeeper coordina el clúster: guarda qué brokers están vivos y quién es el controlador, que es el broker que elige los líderes de cada partición. Son tres para que haya mayoría aunque se caiga uno. Los tres brokers almacenan los eventos y se replican entre ellos. Corren en la misma EC2: toleramos la caída de un broker, no la de la máquina completa.»

## 2:00 – 3:30 · Kafka UI: los dos tópicos, 3 particiones y 3 réplicas

En Kafka UI:

1. **Brokers:** los tres, con su controlador.
2. **Topics → `pedidos-eventos` → Overview:** 3 particiones; en cada una, 3 réplicas, el líder y el ISR completo.
3. **Settings** del mismo tópico: `retention.ms = 604800000` (7 días), `cleanup.policy = delete`, `min.insync.replicas = 2`.
4. Lo mismo en **`pedidos-auditoria`**, con `retention.ms = 2592000000` (30 días).

Para mostrar el líder explícitamente (la guía 3.4 lo hace por consola), en la Terminal 1:

```
sudo docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic pedidos-eventos
```

> «Tres particiones permiten que tres consumidores del mismo grupo procesen en paralelo. Tres réplicas, con `min.insync.replicas=2` y `acks=all` en el productor, hacen que un evento se confirme cuando dos brokers lo tienen: se puede caer uno sin perder escrituras. El líder atiende lecturas y escrituras de cada partición; el ISR son las réplicas que están al día con él.»

> «Usamos `delete` y no `compact` porque la key es el código del pedido: `compact` dejaría solo el último evento de cada pedido, y perderíamos la historia que se audita. La auditoría se retiene más tiempo que los eventos de negocio.»

## 3:30 – 5:30 · Publicación y consumo en vivo

En la aplicación, como Admin, con la pestaña **Kafka → Eventos en vivo** abierta al lado:

1. **Catálogo → Agregar al pedido → Mi pedido → Confirmar pedido.**
2. **Panel Admin → Avanzar Estado** dos o tres veces.

En la pestaña **Kafka**, sin recargar (se actualiza sola cada 3 s), aparece resaltada una fila de **Auditoría** por cada acción: acción, cambio de estado, usuario, **partición y offset**. En **Reportería** el pedido cambia de estado y suma eventos recibidos. Pincha el código del pedido: filtra su historia y se ve que todos sus eventos están en la misma partición con offsets crecientes.

En la **Terminal 2** (`logs-demo`), por cada acción:

```
orders:   EVENTO KAFKA PUBLICADO | Topico: pedidos-eventos   | Particion: 1 | Offset: 7 | Key: PED-... | eventType: PEDIDO_ACEPTADO
orders:   EVENTO KAFKA PUBLICADO | Topico: pedidos-auditoria | Particion: 1 | Offset: 7 | Key: PED-... | eventType: AUDITORIA_PEDIDO
reportes: EVENTO RECIBIDO  | Topico: pedidos-eventos | Particion: 1 | Offset: 7 | ...
reportes: EVENTO PROCESADO | Topico: pedidos-eventos | ... | estado: ACEPTADO
reportes: EVENTO PROCESADO | Topico: pedidos-auditoria | accion: ACEPTAR | usuario: Admin@... | CREADO -> ACEPTADO
```

En **Kafka UI → Topics → `pedidos-eventos` → Messages**: el JSON del evento, con su key y su partición.

Lo mismo que muestra la pestaña Kafka se puede pedir en **Postman → 2. Flujo de eventos**:

```
GET {{urlReportes}}/api/reportes/auditoria?pedido=<codigo>
GET {{urlReportes}}/api/reportes/pedidos/resumen
```

> «El productor no llama al consumidor: publica y sigue. El evento tiene `eventId`, `eventType`, `occurredAt` y `data`, que es la estructura de la guía. El de auditoría agrega quién hizo la acción, tomado del token por el BFF. Como la key es el código del pedido, todos sus eventos caen en la misma partición y llegan en orden: la auditoría muestra CREAR, ACEPTAR, PREPARAR en ese orden y con offsets correlativos. El reporte se actualiza solo, en menos de un segundo.»

> «Se publica después del commit en Oracle. Si el catálogo rechaza el stock y la operación se revierte, Kafka no registra un hecho que no ocurrió.»

## 5:30 – 7:30 · Administrador, consumer groups, offsets, lag y errores

**Administrador Kafka** (Postman, carpeta **4. Administrador Kafka**, las 8 pruebas de la guía 3.4.1): crear `ev3-admin-prueba` con 3 particiones y factor 3, describirlo, cambiar `retention.ms` a 48 horas y comprobarlo, aumentar a 4 particiones y eliminarlo. Mostrar en Kafka UI que aparece y desaparece.

> «Para la eliminación se usa un tópico temporal. Los del caso están protegidos: `DELETE /api/kafka/topics/pedidos-eventos` responde 409.»

**Tópicos** en la aplicación, en **Kafka → Tópicos y particiones**: por cada tópico, las 3 particiones con su líder, réplicas e ISR, y la retención, `cleanup.policy` y `min.insync.replicas`.

**Consumer groups**, en la aplicación, en **Kafka → Consumer groups y lag**: por partición, el miembro que la lee, el offset confirmado, el último offset y el lag. También en Kafka UI → **Consumers → `reportes-pedidos`**, o en la Terminal 1:

```
sudo docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 --describe --group reportes-pedidos
```

> «Cada fila es una partición: el offset que el grupo ya confirmó, el último offset publicado y la diferencia, que es el lag. Tres miembros, uno por partición: así se reparte la carga. Lag en cero significa que el consumidor va al día. Si el lag crece y no baja, el consumidor no da abasto o está detenido; si un grupo queda sin miembros, nadie está procesando; si un miembro acumula todo el lag, la distribución está desigual, por ejemplo porque una key concentra los eventos.»

> «Hay dos grupos: `reportes-pedidos` lee `pedidos-eventos` para la reportería, y `reportes-auditoria` lee `pedidos-auditoria` para la trazabilidad. Cada grupo lleva sus propios offsets: si mañana otro sistema necesitara los mismos eventos, se conecta con su propio grupo y los lee completos sin afectar a estos.»

**Error controlado:** desde la aplicación, como Admin, **Mi pedido** → en el correo del cliente escribe **`error-kafka@pedidos360.cl`** → Confirmar pedido. En la pestaña **Kafka**, a los pocos segundos la tarjeta **Dead Letter Topics** se pone amarilla: `pedidos-eventos-dlt` sube en 1 («+1 desde que abriste el panel»). El pedido sí aparece en Auditoría (otro grupo, otro tópico) pero no en Reportería. En `logs-demo`:

```
reportes: ERROR DE PROCESAMIENTO | Topico: pedidos-eventos | ... | intento 1 de 3 | motivo: Fallo controlado ...
reportes: ERROR DE PROCESAMIENTO | ... | intento 2 de 3
reportes: ERROR DE PROCESAMIENTO | ... | intento 3 de 3
reportes: ENVIADO A DLT | Topico: pedidos-eventos | ... | destino: pedidos-eventos-dlt
```

Y en Kafka UI → **`pedidos-eventos-dlt` → Messages**: el evento, con las cabeceras `kafka_dlt-exception-message` y `kafka_dlt-original-topic`.

> «Se reintenta tres veces con un segundo de espera, y si sigue fallando va al DLT: el consumidor no se queda atascado y los demás pedidos siguen. Un evento inválido —sin campos obligatorios— va directo al DLT sin reintentar, porque fallaría igual. Y un evento repetido se ignora: la tabla `PROCESSED_MESSAGES` registra cada `eventId` en la misma transacción que el reporte. RabbitMQ procesa ese pedido normalmente: el correo solo afecta al consumidor Kafka.»

## 7:30 – 8:30 · Continuidad con RabbitMQ, EP1 y la nube

- En **RabbitMQ Management**: los dos nodos (*Overview*) y las 6 colas (*Queues*). El pedido de la demo también pasó por `q.cmd.email` y `q.cmd.kitchen`, como se ve en `logs-demo` (notify).
- **API Gateway** en la consola de AWS: las rutas `/api/kafka/{proxy+}` y `/api/reportes/{proxy+}` protegidas por el JWT Authorizer, igual que `/api/rabbitmq`. Sin token responden 401.

> «Kafka complementa a RabbitMQ, no lo reemplaza. Todo sigue entrando por el API Gateway con el JWT de Entra ID, el BFF exige el rol Admin para administrar Kafka, y la solución completa corre en la EC2 con Docker Compose.»

## Opcional, si sobra tiempo · Caída de un broker (guía 3.4)

Terminal 1:

```
sudo docker stop kafka-2
sudo docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic pedidos-eventos
```

El ISR baja a dos réplicas (`1,3`) y las particiones que lideraba `kafka-2` pasan a otro broker. Un pedido nuevo igual se publica y se consume. Después:

```
sudo docker start kafka-2
```

En unos segundos el ISR vuelve a `1,2,3`.

> «Con dos réplicas sincronizadas se cumple `min.insync.replicas=2`: el clúster sigue aceptando escrituras. Si el coordinador del consumer group estaba en el broker caído, los consumidores buscan otro y retoman en unos segundos.»

---

## Si te preguntan

**«¿Por qué orders es el único productor?»** Es el microservicio principal del dominio: crea los pedidos y cambia su estado. Los hechos que importan nacen ahí. El catálogo no publica: su stock lo cambia orders al aceptar o cancelar.

**«¿Por qué no se publica desde el controlador o en medio del servicio?»** Para no registrar en Kafka un hecho que después se revierte. `PedidoService` emite un evento interno de Spring y `PublicadorEventosKafka` publica recién cuando la transacción hace commit.

**«¿Qué pasa si Kafka está caído?»** El pedido se confirma igual en Oracle y el error queda en el log con tópico, key y `eventId`. El productor espera como máximo 5 segundos por los metadatos (`max.block.ms`). Garantizar la entrega pese a la caída requeriría un outbox transaccional, que no está implementado.

**«¿Por qué tres particiones y no más?»** El paralelismo de un grupo está limitado por las particiones: con tres, tres consumidores trabajan a la vez. Para el volumen del caso alcanza, y se pueden aumentar después (`PATCH .../partitions`), nunca reducir. Aumentarlas cambia a qué partición va cada key, así que en un tópico ordenado por pedido se hace con cuidado.

**«¿Cuál es la diferencia entre retención en Kafka y en RabbitMQ?»** En RabbitMQ el mensaje sale de la cola al confirmarse. En Kafka el evento se conserva hasta que vence su retención, aunque ya se haya leído; Kafka borra segmentos completos, no mensajes uno por uno. Por eso otro grupo puede leer la historia desde el principio.

**«¿Cuál es la diferencia entre AdminClient y KafkaTemplate?»** `KafkaTemplate` publica eventos (lo usa orders). `AdminClient` administra el clúster: tópicos, configuraciones y grupos (lo usa kafkaadmin). `@KafkaListener` consume (lo usa reportes).

**«¿Qué limita esta arquitectura?»** Los tres brokers están en la misma EC2: protege ante la caída de un broker, no de la máquina. En producción irían en máquinas o zonas distintas.
