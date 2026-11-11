package cl.duoc.pedidos360.reportes.consumer;

import cl.duoc.pedidos360.reportes.model.EventoAuditoria;
import cl.duoc.pedidos360.reportes.model.EventoPedido;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidadorEventosTest {

    private static ValidatorFactory fabrica;
    private static ValidadorEventos validador;

    @BeforeAll
    static void crear() {
        fabrica = Validation.buildDefaultValidatorFactory();
        validador = new ValidadorEventos(fabrica.getValidator());
    }

    @AfterAll
    static void cerrar() {
        fabrica.close();
    }

    static EventoPedido eventoValido() {
        return new EventoPedido("evt-1", "PEDIDO_ACEPTADO", "2026-11-10T18:30:00Z",
                new EventoPedido.Datos(12L, "PED-1", "ACEPTADO", "CREADO", "cliente@duoc.cl",
                        new BigDecimal("15000.00"), 2));
    }

    @Test
    @DisplayName("Un evento completo pasa y devuelve su instante")
    void valido() {
        EventoPedido evento = eventoValido();

        assertThat(validador.validar(evento, evento.occurredAt()))
                .isEqualTo(Instant.parse("2026-11-10T18:30:00Z"));
    }

    @Test
    @DisplayName("Un registro sin contenido se rechaza")
    void sinContenido() {
        assertThatThrownBy(() -> validador.validar(null, null))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("no trae contenido");
    }

    @Test
    @DisplayName("Se informa cada campo que falla, con su ruta")
    void camposFaltantes() {
        EventoPedido evento = new EventoPedido("", "OTRA_COSA", "2026-11-10T18:30:00Z",
                new EventoPedido.Datos(1L, "", "ACEPTADO", null, "no-es-correo", new BigDecimal("-1"), 1));

        assertThatThrownBy(() -> validador.validar(evento, evento.occurredAt()))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("eventId")
                .hasMessageContaining("eventType")
                .hasMessageContaining("data.codigo")
                .hasMessageContaining("data.clienteEmail")
                .hasMessageContaining("data.total");
    }

    @Test
    @DisplayName("Sin data, el evento no se puede procesar")
    void sinDatos() {
        EventoAuditoria registro = new EventoAuditoria("evt-2", "AUDITORIA_PEDIDO", "2026-11-10T18:30:00Z", null);

        assertThatThrownBy(() -> validador.validar(registro, registro.occurredAt()))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("data");
    }

    @Test
    @DisplayName("Una fecha que no es ISO-8601 se rechaza")
    void fechaIlegible() {
        EventoPedido valido = eventoValido();
        EventoPedido evento = new EventoPedido(valido.eventId(), valido.eventType(), "ayer a las 5", valido.data());

        assertThatThrownBy(() -> validador.validar(evento, evento.occurredAt()))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("occurredAt");
    }
}
