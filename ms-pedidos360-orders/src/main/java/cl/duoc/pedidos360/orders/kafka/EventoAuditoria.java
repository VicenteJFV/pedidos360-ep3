package cl.duoc.pedidos360.orders.kafka;

/**
 * Registro de trazabilidad que se publica en pedidos-auditoria.
 *
 * Misma estructura que EventoPedido -eventId, eventType, occurredAt, data-,
 * pero los datos responden otra pregunta: no "que le paso al pedido" sino
 * "quien hizo que, desde donde y sobre que pedido".
 *
 * @param eventType siempre AUDITORIA_PEDIDO; la accion concreta va en data.accion
 */
public record EventoAuditoria(
        String eventId,
        String eventType,
        String occurredAt,
        Datos data
) {

    public static final String TIPO = "AUDITORIA_PEDIDO";

    /**
     * @param accion          CREAR, ACEPTAR, PREPARAR, DESPACHAR, ENTREGAR o CANCELAR
     * @param usuario         quien la hizo, segun el BFF; nunca el token
     * @param origen          microservicio que registro la accion
     * @param detalle         observacion, por ejemplo el motivo de una cancelacion
     * @param eventoNegocioId eventId del evento de pedidos-eventos que origino el
     *                        mismo hecho, para cruzar ambos topicos
     */
    public record Datos(
            String accion,
            String codigoPedido,
            String estadoAnterior,
            String estadoNuevo,
            String usuario,
            String origen,
            String detalle,
            String eventoNegocioId
    ) {
    }
}
