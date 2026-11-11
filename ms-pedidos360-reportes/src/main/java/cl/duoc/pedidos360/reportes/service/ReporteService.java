package cl.duoc.pedidos360.reportes.service;

import cl.duoc.pedidos360.reportes.model.EventoPedido;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reporteria de pedidos, alimentada por pedidos-eventos.
 *
 * Mantiene en REPORTE_PEDIDOS el ultimo estado conocido de cada pedido. Es una
 * vista propia, construida solo con los eventos: no consulta la base de
 * ms-pedidos360-orders. Asi la reporteria no carga al microservicio principal
 * y sigue respondiendo aunque orders este caido.
 */
@Service
public class ReporteService {

    private final JdbcTemplate jdbc;
    private final RegistroIdempotente idempotencia;
    private final DisparadorDeFallas disparador;

    public ReporteService(JdbcTemplate jdbc, RegistroIdempotente idempotencia, DisparadorDeFallas disparador) {
        this.jdbc = jdbc;
        this.idempotencia = idempotencia;
        this.disparador = disparador;
    }

    /**
     * Una sola transaccion para la marca de idempotencia y el reporte: o quedan
     * las dos, o ninguna.
     */
    @Transactional
    public Resultado registrar(EventoPedido evento, Instant ocurrido) {
        EventoPedido.Datos datos = evento.data();

        // El fallo controlado va antes de tocar la base, como en la guia: un
        // evento que falla nunca queda marcado como procesado.
        disparador.evaluar(datos.clienteEmail(), evento.eventId());

        if (!idempotencia.marcarProcesado(evento.eventId())) {
            return Resultado.DUPLICADO;
        }

        Timestamp instante = Timestamp.from(ocurrido);

        // Solo avanza si el evento es igual o mas nuevo que lo que ya se sabe del
        // pedido. Dentro de una particion los eventos de un pedido llegan en
        // orden (la key es el codigo); esta condicion cubre el reproceso de un
        // evento viejo desde el DLT, que no debe hacer retroceder el estado.
        int actualizadas = jdbc.update("""
                UPDATE REPORTE_PEDIDOS
                   SET ESTADO = ?, CLIENTE_EMAIL = ?, TOTAL = ?, CANTIDAD_ITEMS = ?,
                       PEDIDO_ID = ?, ULTIMO_EVENTO_EN = ?, EVENTOS_RECIBIDOS = EVENTOS_RECIBIDOS + 1
                 WHERE CODIGO = ? AND ULTIMO_EVENTO_EN <= ?
                """,
                datos.estado(), datos.clienteEmail(), datos.total(), datos.cantidadItems(),
                datos.pedidoId(), instante, datos.codigo(), instante);
        if (actualizadas == 1) {
            return Resultado.PROCESADO;
        }

        Integer existe = jdbc.queryForObject(
                "SELECT COUNT(*) FROM REPORTE_PEDIDOS WHERE CODIGO = ?", Integer.class, datos.codigo());
        if (existe != null && existe > 0) {
            jdbc.update("UPDATE REPORTE_PEDIDOS SET EVENTOS_RECIBIDOS = EVENTOS_RECIBIDOS + 1 WHERE CODIGO = ?",
                    datos.codigo());
            return Resultado.ANTERIOR_AL_ESTADO_ACTUAL;
        }

        jdbc.update("""
                INSERT INTO REPORTE_PEDIDOS
                       (CODIGO, PEDIDO_ID, ESTADO, CLIENTE_EMAIL, TOTAL, CANTIDAD_ITEMS, ULTIMO_EVENTO_EN, EVENTOS_RECIBIDOS)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                datos.codigo(), datos.pedidoId(), datos.estado(), datos.clienteEmail(), datos.total(),
                datos.cantidadItems(), instante);
        return Resultado.PROCESADO;
    }

    /**
     * Cuantos pedidos hay en cada estado y cuanto suman. "ventasEntregadas" es
     * el monto de los pedidos que ya llegaron al cliente.
     */
    @Transactional(readOnly = true)
    public Map<String, Object> resumen() {
        Map<String, Object> porEstado = new LinkedHashMap<>();
        BigDecimal ventasEntregadas = BigDecimal.ZERO;
        long total = 0;

        List<Map<String, Object>> filas = jdbc.queryForList("""
                SELECT ESTADO, COUNT(*) AS CANTIDAD, COALESCE(SUM(TOTAL), 0) AS MONTO
                  FROM REPORTE_PEDIDOS GROUP BY ESTADO ORDER BY ESTADO
                """);
        for (Map<String, Object> fila : filas) {
            String estado = (String) fila.get("ESTADO");
            long cantidad = ((Number) fila.get("CANTIDAD")).longValue();
            BigDecimal monto = new BigDecimal(fila.get("MONTO").toString());
            porEstado.put(estado, Map.of("pedidos", cantidad, "monto", monto));
            total += cantidad;
            if ("ENTREGADO".equals(estado)) {
                ventasEntregadas = monto;
            }
        }

        Map<String, Object> resumen = new LinkedHashMap<>();
        resumen.put("totalPedidos", total);
        resumen.put("ventasEntregadas", ventasEntregadas);
        resumen.put("porEstado", porEstado);
        Timestamp ultimo = jdbc.queryForObject("SELECT MAX(ULTIMO_EVENTO_EN) FROM REPORTE_PEDIDOS", Timestamp.class);
        resumen.put("ultimoEvento", ultimo == null ? null : ultimo.toInstant());
        return resumen;
    }

    /** Una fila de REPORTE_PEDIDOS. */
    public record FilaReporte(String codigo, Long pedidoId, String estado, String clienteEmail,
                              BigDecimal total, Integer cantidadItems, Instant ultimoEvento,
                              int eventosRecibidos) {
    }

    private static final RowMapper<FilaReporte> FILA = (rs, n) -> new FilaReporte(
            rs.getString("CODIGO"), rs.getObject("PEDIDO_ID") == null ? null : rs.getLong("PEDIDO_ID"),
            rs.getString("ESTADO"), rs.getString("CLIENTE_EMAIL"), rs.getBigDecimal("TOTAL"),
            rs.getObject("CANTIDAD_ITEMS") == null ? null : rs.getInt("CANTIDAD_ITEMS"),
            rs.getTimestamp("ULTIMO_EVENTO_EN").toInstant(), rs.getInt("EVENTOS_RECIBIDOS"));

    /** Los ultimos pedidos con actividad, opcionalmente filtrados por estado. */
    @Transactional(readOnly = true)
    public List<FilaReporte> pedidos(String estado) {
        String sql = """
                SELECT CODIGO, PEDIDO_ID, ESTADO, CLIENTE_EMAIL, TOTAL, CANTIDAD_ITEMS,
                       ULTIMO_EVENTO_EN, EVENTOS_RECIBIDOS
                  FROM REPORTE_PEDIDOS
                """;
        if (estado != null) {
            return jdbc.query(sql + " WHERE ESTADO = ? ORDER BY ULTIMO_EVENTO_EN DESC FETCH FIRST 100 ROWS ONLY",
                    FILA, estado);
        }
        return jdbc.query(sql + " ORDER BY ULTIMO_EVENTO_EN DESC FETCH FIRST 100 ROWS ONLY", FILA);
    }
}
