package pedidos360.bff.controllers;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

/**
 * Reenvio tal cual de una peticion hacia un microservicio interno: metodo, ruta,
 * query string y cuerpo.
 *
 * Lo usan los controladores que exponen APIs administrativas a traves del BFF
 * (RabbitMQ en EP2; Kafka y reportes en EP3). Ninguno agrega logica: la
 * validacion es del microservicio de destino, y su 400 llega al cliente sin
 * cambios. Lo que si agrega el BFF, en cada controlador, es la autorizacion
 * por rol.
 */
final class Reenvio {

    private Reenvio() {
    }

    static ResponseEntity<String> reenviar(RestTemplate restTemplate, String baseUrl,
                                           HttpServletRequest peticion, String cuerpo) {

        String ruta = peticion.getRequestURI().substring(peticion.getContextPath().length());
        String consulta = peticion.getQueryString();

        // URI ya codificada, tal como llego. Si se pasara como String, RestTemplate
        // la volveria a codificar y un queueName=%20 llegaria como %2520.
        URI destino = URI.create(baseUrl + ruta + (consulta != null ? "?" + consulta : ""));

        HttpHeaders cabeceras = new HttpHeaders();
        if (cuerpo != null) {
            cabeceras.setContentType(MediaType.APPLICATION_JSON);
        }

        try {
            ResponseEntity<String> respuesta = restTemplate.exchange(destino,
                    HttpMethod.valueOf(peticion.getMethod()),
                    new HttpEntity<>(cuerpo, cabeceras), String.class);
            return ResponseEntity.status(respuesta.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(respuesta.getBody());
        } catch (HttpStatusCodeException e) {
            // Solo el estado y el cuerpo. No se copian las cabeceras de la respuesta
            // del microservicio: Transfer-Encoding y Connection son hop-by-hop, y
            // reenviarlas rompia la respuesta en EP1 (500 con cuerpo vacio).
            return ResponseEntity.status(e.getStatusCode())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(e.getResponseBodyAsString());
        }
    }
}
