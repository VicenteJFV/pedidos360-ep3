package cl.duoc.pedidos360.reportes.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Registro de pedidos-auditoria, del lado del consumidor.
 *
 * Misma estructura que EventoPedido; "data" responde quien hizo que, desde
 * donde y sobre que pedido.
 */
public record EventoAuditoria(
        @NotBlank String eventId,
        @NotBlank String eventType,
        @NotBlank String occurredAt,
        @NotNull @Valid Datos data
) {

    public record Datos(
            @NotBlank String accion,
            @NotBlank String codigoPedido,
            String estadoAnterior,
            @NotBlank String estadoNuevo,
            @NotBlank @Size(max = 150) String usuario,
            @NotBlank String origen,
            @Size(max = 500) String detalle,
            String eventoNegocioId
    ) {
    }
}
