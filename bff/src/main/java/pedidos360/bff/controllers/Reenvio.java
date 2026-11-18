package pedidos360.bff.controllers;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;

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

    /**
     * El RestTemplate de los reenvios administrativos.
     *
     * Usa el cliente HTTP de java.net.http y no el HttpURLConnection por defecto,
     * que no admite PATCH: la API de la guia 3.4.1 modifica configuraciones y
     * particiones con PATCH, y por el BFF respondia 500 ("Invalid HTTP method:
     * PATCH"). HTTP/1.1 explicito para que el cliente no intente subir a HTTP/2
     * contra los microservicios.
     */
    static RestTemplate nuevoRestTemplate() {
        HttpClient cliente = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        return new RestTemplate(new JdkClientHttpRequestFactory(cliente));
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
