package cl.duoc.pedidos360.reportes.service;

import cl.duoc.pedidos360.reportes.model.EventoAuditoria;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Auditoria de pedidos, alimentada por pedidos-auditoria.
 *
 * Una fila por accion, en orden: es la historia del pedido, no su ultimo
 * estado. Guarda la particion y el offset en que llego cada registro para poder
 * cruzarlo con lo que muestra Kafka UI.
 */
@Service
public class AuditoriaService {

    private final JdbcTemplate jdbc;
    private final RegistroIdempotente idempotencia;

    public AuditoriaService(JdbcTemplate jdbc, RegistroIdempotente idempotencia) {
        this.jdbc = jdbc;
        this.idempotencia = idempotencia;
    }

    @Transactional
    public Resultado registrar(EventoAuditoria registro, Instant ocurrido, int particion, long offset) {
        if (!idempotencia.marcarProcesado(registro.eventId())) {
            return Resultado.DUPLICADO;
        }
        EventoAuditoria.Datos datos = registro.data();
        jdbc.update("""
                INSERT INTO AUDITORIA_PEDIDOS
                       (EVENT_ID, CODIGO_PEDIDO, ACCION, ESTADO_ANTERIOR, ESTADO_NUEVO, USUARIO, ORIGEN,
                        DETALLE, EVENTO_NEGOCIO_ID, OCURRIDO_EN, PARTICION, OFFSET_KAFKA)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                registro.eventId(), datos.codigoPedido(), datos.accion(), datos.estadoAnterior(),
                datos.estadoNuevo(), datos.usuario(), datos.origen(), datos.detalle(),
                datos.eventoNegocioId(), Timestamp.from(ocurrido), particion, offset);
        return Resultado.PROCESADO;
    }

    /** Una fila de AUDITORIA_PEDIDOS. */
    public record FilaAuditoria(String eventId, String codigoPedido, String accion, String estadoAnterior,
                                String estadoNuevo, String usuario, String origen, String detalle,
                                String eventoNegocioId, Instant ocurridoEn, Integer particion, Long offset) {
    }

    private static final RowMapper<FilaAuditoria> FILA = (rs, n) -> new FilaAuditoria(
            rs.getString("EVENT_ID"), rs.getString("CODIGO_PEDIDO"), rs.getString("ACCION"),
            rs.getString("ESTADO_ANTERIOR"), rs.getString("ESTADO_NUEVO"), rs.getString("USUARIO"),
            rs.getString("ORIGEN"), rs.getString("DETALLE"), rs.getString("EVENTO_NEGOCIO_ID"),
            rs.getTimestamp("OCURRIDO_EN").toInstant(),
            rs.getObject("PARTICION") == null ? null : rs.getInt("PARTICION"),
            rs.getObject("OFFSET_KAFKA") == null ? null : rs.getLong("OFFSET_KAFKA"));

    /** La historia de un pedido, o las ultimas 100 acciones si no se indica pedido. */
    @Transactional(readOnly = true)
    public List<FilaAuditoria> historial(String codigoPedido) {
        String sql = """
                SELECT EVENT_ID, CODIGO_PEDIDO, ACCION, ESTADO_ANTERIOR, ESTADO_NUEVO, USUARIO, ORIGEN,
                       DETALLE, EVENTO_NEGOCIO_ID, OCURRIDO_EN, PARTICION, OFFSET_KAFKA
                  FROM AUDITORIA_PEDIDOS
                """;
        if (codigoPedido != null) {
            return jdbc.query(sql + " WHERE CODIGO_PEDIDO = ? ORDER BY OCURRIDO_EN", FILA, codigoPedido);
        }
        return jdbc.query(sql + " ORDER BY OCURRIDO_EN DESC FETCH FIRST 100 ROWS ONLY", FILA);
    }
}
