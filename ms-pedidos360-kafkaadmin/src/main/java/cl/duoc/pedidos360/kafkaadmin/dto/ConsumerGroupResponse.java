package cl.duoc.pedidos360.kafkaadmin.dto;

/**
 * Una fila de GET /api/kafka/consumer-groups: los campos de la guia 3.4.1
 * (groupId, simpleConsumerGroup) mas el estado y cuantos miembros tiene.
 *
 * @param state STABLE (consumiendo), EMPTY (sin miembros: nadie consume),
 *              PREPARING_REBALANCE / COMPLETING_REBALANCE (reasignando particiones)
 */
public record ConsumerGroupResponse(String groupId, boolean simpleConsumerGroup, String state, int members) {
}
