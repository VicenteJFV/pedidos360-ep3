package cl.duoc.pedidos360.orders.service;

import cl.duoc.pedidos360.orders.domain.EstadoPedido;
import cl.duoc.pedidos360.orders.domain.Pedido;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Hecho del dominio: un pedido se creo o cambio de estado.
 *
 * Es un evento interno de Spring, no un mensaje Kafka. PedidoService lo emite y
 * no sabe quien lo escucha; PublicadorEventosKafka lo recibe despues del commit
 * y lo convierte en los eventos de los dos topicos. Asi la logica de negocio
 * queda separada de la configuracion de mensajeria (seccion 5.3 de la guia).
 *
 * Lleva una foto del pedido tomada dentro de la transaccion, porque despues del
 * commit la entidad ya no puede cargar sus relaciones.
 *
 * @param accion         que se hizo: CREAR, ACEPTAR, PREPARAR, DESPACHAR,
 *                       ENTREGAR o CANCELAR
 * @param estadoAnterior null cuando el pedido recien se crea
 * @param usuario        quien hizo la accion, segun el BFF; "sistema" si no se sabe
 * @param motivo         observacion de la accion, por ejemplo el motivo de cancelacion
 */
public record CambioEstadoPedido(
        String accion,
        Long pedidoId,
        String codigo,
        EstadoPedido estadoAnterior,
        EstadoPedido estadoNuevo,
        String clienteEmail,
        BigDecimal total,
        int cantidadItems,
        String usuario,
        String motivo,
        Instant ocurridoEn
) {

    static CambioEstadoPedido de(String accion, Pedido pedido, EstadoPedido estadoAnterior,
                                 String usuario, String motivo) {
        return new CambioEstadoPedido(
                accion,
                pedido.getId(),
                pedido.getCodigo(),
                estadoAnterior,
                pedido.getEstado(),
                pedido.getClienteEmail(),
                pedido.getTotal(),
                pedido.getItems().size(),
                usuario,
                motivo,
                Instant.now());
    }
}
