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
 * Reenvia la API del microservicio administrador de RabbitMQ (EP2).
 *
 * ms-pedidos360-rabbitadmin escucha solo dentro de la EC2 y no tiene
 * autenticacion propia: puede crear y eliminar colas, exchanges y bindings. Por
 * eso no se publica directo, sino que entra por el mismo camino que el resto de
 * la API -API Gateway con JWT, y el BFF con autorizacion por rol- y solo para
 * Admin. Es el mismo modelo de EP1: el Gateway valida el token, el BFF decide
 * que puede hacer cada rol y los microservicios no ven el token.
 *
 * Reenvia todo lo que cuelga de /api/rabbitmq tal cual -metodo, ruta, query
 * string y cuerpo- en lugar de declarar un metodo por endpoint, porque el BFF no
 * agrega logica a estas operaciones: la validacion de parametros es de
 * rabbitadmin, y su 400 llega al cliente sin cambios.
 */
@RestController
@RequestMapping("/api/rabbitmq")
@PreAuthorize("hasAuthority('ROLE_Admin')")
public class RabbitAdminController {

    private final String rabbitAdminBaseUrl;

    /** Package-private para poder reemplazarlo por un servidor simulado en las pruebas. */
    final RestTemplate restTemplate = Reenvio.nuevoRestTemplate();

    public RabbitAdminController(
            @Value("${microservicios.rabbitadmin.base-url:http://127.0.0.1:8084}") String rabbitAdminBaseUrl) {
        this.rabbitAdminBaseUrl = rabbitAdminBaseUrl;
    }

    @RequestMapping({"", "/**"})
    public ResponseEntity<String> reenviar(HttpServletRequest peticion,
                                           @RequestBody(required = false) String cuerpo) {
        return Reenvio.reenviar(restTemplate, rabbitAdminBaseUrl, peticion, cuerpo);
    }
}
