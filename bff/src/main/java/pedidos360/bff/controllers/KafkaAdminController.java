package pedidos360.bff.controllers;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

/**
 * Reenvia la API del microservicio administrador Kafka (EP3).
 *
 * Mismo modelo que RabbitAdminController: ms-pedidos360-kafkaadmin puede crear,
 * modificar y eliminar topicos y no tiene autenticacion propia, asi que solo se
 * llega a el por el API Gateway (JWT) y el BFF, y solo con el rol Admin.
 */
@RestController
@RequestMapping("/api/kafka")
@PreAuthorize("hasAuthority('ROLE_Admin')")
public class KafkaAdminController {

    private final String kafkaAdminBaseUrl;

    /** Package-private para poder reemplazarlo por un servidor simulado en las pruebas. */
    final RestTemplate restTemplate = new RestTemplate();

    public KafkaAdminController(
            @Value("${microservicios.kafkaadmin.base-url:http://127.0.0.1:8086}") String kafkaAdminBaseUrl) {
        this.kafkaAdminBaseUrl = kafkaAdminBaseUrl;
    }

    @RequestMapping({"", "/**"})
    public ResponseEntity<String> reenviar(HttpServletRequest peticion,
                                           @RequestBody(required = false) String cuerpo) {
        return Reenvio.reenviar(restTemplate, kafkaAdminBaseUrl, peticion, cuerpo);
    }
}
