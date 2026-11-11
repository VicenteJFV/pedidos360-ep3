package cl.duoc.pedidos360.reportes.web;

import cl.duoc.pedidos360.reportes.service.AuditoriaService;
import cl.duoc.pedidos360.reportes.service.ReporteService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Lo que la reporteria y la auditoria construyeron a partir de los eventos.
 *
 * Se consulta a traves del BFF, que exige el rol Admin, igual que la
 * administracion de RabbitMQ y de Kafka.
 */
@RestController
@RequestMapping("/api/reportes")
public class ReporteController {

    private static final Set<String> ESTADOS = Set.of(
            "CREADO", "ACEPTADO", "EN_PREPARACION", "DESPACHADO", "ENTREGADO", "CANCELADO");

    private final ReporteService reportes;
    private final AuditoriaService auditoria;

    public ReporteController(ReporteService reportes, AuditoriaService auditoria) {
        this.reportes = reportes;
        this.auditoria = auditoria;
    }

    /** Pedidos por estado, montos y momento del ultimo evento recibido. */
    @GetMapping("/pedidos/resumen")
    public Map<String, Object> resumen() {
        return reportes.resumen();
    }

    @GetMapping("/pedidos")
    public ResponseEntity<?> pedidos(@RequestParam(required = false) String estado) {
        if (!StringUtils.hasText(estado)) {
            return ResponseEntity.ok(reportes.pedidos(null));
        }
        String normalizado = estado.trim().toUpperCase(Locale.ROOT);
        if (!ESTADOS.contains(normalizado)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "estado no valido: " + estado,
                    "parametro", "estado",
                    "valoresPermitidos", ESTADOS.stream().sorted().toList()));
        }
        return ResponseEntity.ok(reportes.pedidos(normalizado));
    }

    /** Historia de acciones de un pedido, o las ultimas 100 si no se indica. */
    @GetMapping("/auditoria")
    public Object auditoria(@RequestParam(required = false) String pedido) {
        return auditoria.historial(StringUtils.hasText(pedido) ? pedido.trim() : null);
    }
}
