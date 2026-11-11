package cl.duoc.pedidos360.orders.kafka;

import java.math.BigDecimal;

/**
 * Evento del negocio que se publica en pedidos-eventos.
 *
 * Sigue la estructura de la seccion 5.4 de la guia de EP3: identificador del
 * evento, tipo, fecha y hora, y los datos del dominio en "data".
 *
 * <pre>
 * {
 *   "eventId": "6f1c...",
 *   "eventType": "PEDIDO_DESPACHADO",
 *   "occurredAt": "2026-11-10T18:30:00Z",
 *   "data": { "pedidoId": 12, "codigo": "PED-20261110-AB12CD34", "estado": "DESPACHADO", ... }
 * }
 * </pre>
 *
 * @param eventType  PEDIDO_ + el estado nuevo: PEDIDO_CREADO, PEDIDO_ACEPTADO,
 *                   PEDIDO_EN_PREPARACION, PEDIDO_DESPACHADO, PEDIDO_ENTREGADO o
 *                   PEDIDO_CANCELADO
 * @param occurredAt instante en formato ISO-8601 (UTC), como en el ejemplo de la guia
 */
public record EventoPedido(
        String eventId,
        String eventType,
        String occurredAt,
        Datos data
) {

    /**
     * @param estadoAnterior null en PEDIDO_CREADO
     * @param cantidadItems  lineas del pedido; la reporteria no necesita el detalle
     */
    public record Datos(
            Long pedidoId,
            String codigo,
            String estado,
            String estadoAnterior,
            String clienteEmail,
            BigDecimal total,
            Integer cantidadItems
    ) {
    }
}
