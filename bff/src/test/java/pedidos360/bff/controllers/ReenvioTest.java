package pedidos360.bff.controllers;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El reenvio con un servidor HTTP real, no simulado.
 *
 * Las pruebas de los controladores reemplazan el cliente HTTP del RestTemplate
 * por MockRestServiceServer, y por eso no notaron que el cliente por defecto
 * (HttpURLConnection) no admite PATCH: en la EC2, PATCH .../configs respondia
 * 500. Esta prueba usa el cliente de verdad contra un servidor de verdad.
 */
class ReenvioTest {

    @Test
    @DisplayName("PATCH llega al microservicio con su metodo y su cuerpo")
    void reenviaPatch() throws Exception {
        AtomicReference<String> metodo = new AtomicReference<>();
        AtomicReference<String> cuerpo = new AtomicReference<>();
        HttpServer servidor = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        servidor.createContext("/", intercambio -> {
            metodo.set(intercambio.getRequestMethod());
            cuerpo.set(new String(intercambio.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] respuesta = "{\"message\":\"Configuracion actualizada\"}".getBytes(StandardCharsets.UTF_8);
            intercambio.getResponseHeaders().add("Content-Type", "application/json");
            intercambio.sendResponseHeaders(200, respuesta.length);
            intercambio.getResponseBody().write(respuesta);
            intercambio.close();
        });
        servidor.start();
        try {
            MockHttpServletRequest peticion = new MockHttpServletRequest(HttpMethod.PATCH.name(),
                    "/api/kafka/topics/ev3-admin-prueba/configs");
            String json = "{\"configName\":\"retention.ms\",\"value\":\"172800000\"}";

            ResponseEntity<String> respuesta = Reenvio.reenviar(Reenvio.nuevoRestTemplate(),
                    "http://127.0.0.1:" + servidor.getAddress().getPort(), peticion, json);

            assertThat(respuesta.getStatusCode().value()).isEqualTo(200);
            assertThat(metodo.get()).isEqualTo("PATCH");
            assertThat(cuerpo.get()).isEqualTo(json);
        } finally {
            servidor.stop(0);
        }
    }
}
