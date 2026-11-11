package cl.duoc.pedidos360.reportes.service;

import cl.duoc.pedidos360.reportes.model.EventoAuditoria;
import cl.duoc.pedidos360.reportes.model.EventoPedido;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reporteria y auditoria contra la base H2 del perfil dev. Los listeners no se
 * inician: aqui se prueba la logica, sin Kafka.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.kafka.listener.auto-startup=false")
class ServiciosTest {

    @Autowired
    private ReporteService reportes;

    @Autowired
    private AuditoriaService auditoria;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void limpiar() {
        jdbc.update("DELETE FROM PROCESSED_MESSAGES");
        jdbc.update("DELETE FROM REPORTE_PEDIDOS");
        jdbc.update("DELETE FROM AUDITORIA_PEDIDOS");
    }

    private static EventoPedido evento(String codigo, String estado, String email, String instante) {
        return new EventoPedido(UUID.randomUUID().toString(), "PEDIDO_" + estado, instante,
                new EventoPedido.Datos(7L, codigo, estado, null, email, new BigDecimal("15000.00"), 1));
    }

    private String estadoDe(String codigo) {
        return jdbc.queryForObject("SELECT ESTADO FROM REPORTE_PEDIDOS WHERE CODIGO = ?", String.class, codigo);
    }

    @Test
    @DisplayName("Los eventos de un pedido van actualizando su ultimo estado")
    void avanzaElEstado() {
        EventoPedido creado = evento("PED-1", "CREADO", "a@duoc.cl", "2026-11-10T10:00:00Z");
        EventoPedido aceptado = evento("PED-1", "ACEPTADO", "a@duoc.cl", "2026-11-10T10:05:00Z");

        assertThat(reportes.registrar(creado, Instant.parse(creado.occurredAt()))).isEqualTo(Resultado.PROCESADO);
        assertThat(reportes.registrar(aceptado, Instant.parse(aceptado.occurredAt()))).isEqualTo(Resultado.PROCESADO);

        assertThat(estadoDe("PED-1")).isEqualTo("ACEPTADO");
        assertThat(jdbc.queryForObject("SELECT EVENTOS_RECIBIDOS FROM REPORTE_PEDIDOS WHERE CODIGO = 'PED-1'",
                Integer.class)).isEqualTo(2);
    }

    @Test
    @DisplayName("El mismo evento dos veces se procesa una sola vez (idempotencia)")
    void idempotencia() {
        EventoPedido creado = evento("PED-2", "CREADO", "a@duoc.cl", "2026-11-10T10:00:00Z");

        assertThat(reportes.registrar(creado, Instant.parse(creado.occurredAt()))).isEqualTo(Resultado.PROCESADO);
        assertThat(reportes.registrar(creado, Instant.parse(creado.occurredAt()))).isEqualTo(Resultado.DUPLICADO);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM PROCESSED_MESSAGES WHERE EVENT_ID = ?",
                Integer.class, creado.eventId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT EVENTOS_RECIBIDOS FROM REPORTE_PEDIDOS WHERE CODIGO = 'PED-2'",
                Integer.class)).isEqualTo(1);
    }

    @Test
    @DisplayName("Un evento mas antiguo que el ultimo estado no lo hace retroceder")
    void noRetrocede() {
        EventoPedido aceptado = evento("PED-3", "ACEPTADO", "a@duoc.cl", "2026-11-10T10:05:00Z");
        EventoPedido creadoTardio = evento("PED-3", "CREADO", "a@duoc.cl", "2026-11-10T10:00:00Z");

        reportes.registrar(aceptado, Instant.parse(aceptado.occurredAt()));
        assertThat(reportes.registrar(creadoTardio, Instant.parse(creadoTardio.occurredAt())))
                .isEqualTo(Resultado.ANTERIOR_AL_ESTADO_ACTUAL);

        assertThat(estadoDe("PED-3")).isEqualTo("ACEPTADO");
    }

    @Test
    @DisplayName("El fallo controlado no deja nada registrado, ni siquiera la marca de idempotencia")
    void falloControladoNoDejaRastro() {
        EventoPedido evento = evento("PED-4", "CREADO", "error-kafka@pedidos360.cl", "2026-11-10T10:00:00Z");

        assertThatThrownBy(() -> reportes.registrar(evento, Instant.parse(evento.occurredAt())))
                .isInstanceOf(DisparadorDeFallas.FalloControladoException.class);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM PROCESSED_MESSAGES", Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM REPORTE_PEDIDOS", Integer.class)).isZero();
    }

    @Test
    @DisplayName("El resumen cuenta pedidos por estado y suma lo entregado")
    @SuppressWarnings("unchecked")
    void resumen() {
        reportes.registrar(evento("PED-5", "ENTREGADO", "a@duoc.cl", "2026-11-10T10:00:00Z"),
                Instant.parse("2026-11-10T10:00:00Z"));
        reportes.registrar(evento("PED-6", "ENTREGADO", "a@duoc.cl", "2026-11-10T10:01:00Z"),
                Instant.parse("2026-11-10T10:01:00Z"));
        reportes.registrar(evento("PED-7", "CREADO", "a@duoc.cl", "2026-11-10T10:02:00Z"),
                Instant.parse("2026-11-10T10:02:00Z"));

        Map<String, Object> resumen = reportes.resumen();

        assertThat(resumen.get("totalPedidos")).isEqualTo(3L);
        assertThat((BigDecimal) resumen.get("ventasEntregadas")).isEqualByComparingTo("30000.00");
        assertThat((Map<String, Object>) resumen.get("porEstado")).containsOnlyKeys("CREADO", "ENTREGADO");
        assertThat(resumen.get("ultimoEvento")).isEqualTo(Instant.parse("2026-11-10T10:02:00Z"));
    }

    @Test
    @DisplayName("La auditoria guarda cada accion en orden, con particion y offset")
    void auditoria() {
        EventoAuditoria crear = new EventoAuditoria("aud-1", "AUDITORIA_PEDIDO", "2026-11-10T10:00:00Z",
                new EventoAuditoria.Datos("CREAR", "PED-8", null, "CREADO", "Admin@duoc.cl",
                        "ms-pedidos360-orders", null, "evt-1"));
        EventoAuditoria aceptar = new EventoAuditoria("aud-2", "AUDITORIA_PEDIDO", "2026-11-10T10:05:00Z",
                new EventoAuditoria.Datos("ACEPTAR", "PED-8", "CREADO", "ACEPTADO", "Admin@duoc.cl",
                        "ms-pedidos360-orders", null, "evt-2"));

        auditoria.registrar(aceptar, Instant.parse(aceptar.occurredAt()), 1, 41);
        auditoria.registrar(crear, Instant.parse(crear.occurredAt()), 1, 40);
        assertThat(auditoria.registrar(crear, Instant.parse(crear.occurredAt()), 1, 40))
                .isEqualTo(Resultado.DUPLICADO);

        assertThat(auditoria.historial("PED-8"))
                .extracting(AuditoriaService.FilaAuditoria::accion, AuditoriaService.FilaAuditoria::offset)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("CREAR", 40L),
                        org.assertj.core.groups.Tuple.tuple("ACEPTAR", 41L));
    }
}
