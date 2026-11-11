package cl.duoc.pedidos360.reportes.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;

import java.math.BigDecimal;

/**
 * Evento de pedidos-eventos, del lado del consumidor.
 *
 * Es el mismo JSON que publica ms-pedidos360-orders, con la estructura de la
 * seccion 5.4 de la guia. El consumidor tiene su propia clase -no comparte la
 * del productor-: el contrato entre ambos es el JSON, y cada microservicio
 * puede cambiar su codigo sin obligar al otro a recompilar.
 *
 * Las anotaciones son las reglas que el consumidor exige antes de procesar,
 * como el EventoDTO de la guia 3.3.2. Las aplica ValidadorEventos.
 */
public record EventoPedido(
        @NotBlank String eventId,
        @NotBlank @Pattern(regexp = "PEDIDO_[A-Z_]+", message = "debe tener la forma PEDIDO_<ESTADO>")
        String eventType,
        @NotBlank String occurredAt,
        @NotNull @Valid Datos data
) {

    public record Datos(
            Long pedidoId,
            @NotBlank String codigo,
            @NotBlank String estado,
            String estadoAnterior,
            @Email String clienteEmail,
            @PositiveOrZero BigDecimal total,
            @PositiveOrZero Integer cantidadItems
    ) {
    }
}
