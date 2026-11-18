package pedidos360.bff.controllers;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

/**
 * Reenvia las consultas de reporteria y auditoria de ms-pedidos360-reportes (EP3).
 *
 * Solo lectura (GET) y solo para Admin: la auditoria muestra quien hizo cada
 * accion sobre todos los pedidos, y eso no lo debe ver un Cliente.
 */
@RestController
@RequestMapping("/api/reportes")
@PreAuthorize("hasAuthority('ROLE_Admin')")
public class ReportesController {

    private final String reportesBaseUrl;

    /** Package-private para poder reemplazarlo por un servidor simulado en las pruebas. */
    final RestTemplate restTemplate = Reenvio.nuevoRestTemplate();

    public ReportesController(
            @Value("${microservicios.reportes.base-url:http://127.0.0.1:8085}") String reportesBaseUrl) {
        this.reportesBaseUrl = reportesBaseUrl;
    }

    @GetMapping({"", "/**"})
    public ResponseEntity<String> reenviar(HttpServletRequest peticion) {
        return Reenvio.reenviar(restTemplate, reportesBaseUrl, peticion, null);
    }
}
