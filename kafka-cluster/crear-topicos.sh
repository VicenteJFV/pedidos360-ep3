#!/bin/bash
# Crea los topicos de Pedidos360 en el cluster Kafka de tres brokers.
#
# Son los dos topicos de la seccion 4 de la guia de EP3, cada uno con
# 3 particiones y factor de replicacion 3, mas un Dead Letter Topic (DLT) por
# cada uno, como el eventos-entrada-dlt de la guia practica 3.3.2.
#
# Retencion y limpieza (seccion 6.1 de la guia):
#
#   pedidos-eventos      7 dias   delete   Alimenta la reporteria. Una semana da
#                                          margen para reprocesar si el consumidor
#                                          estuvo caido o si hubo un error.
#   pedidos-auditoria   30 dias   delete   La trazabilidad tiene que durar mas
#                                          que el dato operativo.
#   *-dlt               14 dias   delete   Tiempo para revisar y reprocesar los
#                                          eventos que fallaron.
#
# cleanup.policy=delete y no compact: compact deja solo el ultimo registro de
# cada key, y aqui la key es el codigo del pedido. Con compact se perderia la
# historia de estados del pedido, que es justamente lo que se audita.
#
# min.insync.replicas=2: con acks=all en el productor, un evento se confirma
# cuando lo tienen al menos 2 de las 3 replicas.
set -e

BOOTSTRAP=kafka-1:9092,kafka-2:9092,kafka-3:9092

crear() {
  local topico=$1 retencion=$2
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topico" --partitions 3 --replication-factor 3 \
    --config retention.ms="$retencion" \
    --config cleanup.policy=delete \
    --config min.insync.replicas=2
}

crear pedidos-eventos       604800000
crear pedidos-auditoria    2592000000
crear pedidos-eventos-dlt  1209600000
crear pedidos-auditoria-dlt 1209600000

echo "Topicos del caso:"
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic 'pedidos-.*' | grep -E '^Topic:'
