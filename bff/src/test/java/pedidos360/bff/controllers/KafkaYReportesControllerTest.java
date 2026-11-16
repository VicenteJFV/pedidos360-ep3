package pedidos360.bff.controllers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.util.AopTestUtils;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.servlet.MockMvc;
import pedidos360.bff.config.FiltroContentLength;
import pedidos360.bff.security.SecurityConfig;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Reenvio de /api/kafka hacia ms-pedidos360-kafkaadmin y de /api/reportes hacia
 * ms-pedidos360-reportes (EP3). Mismo esquema que RabbitAdminControllerTest.
 */
@WebMvcTest({KafkaAdminController.class, ReportesController.class})
@Import({SecurityConfig.class, FiltroContentLength.class})
class KafkaYReportesControllerTest {

    private static final String KAFKAADMIN = "http://127.0.0.1:8086";
    private static final String REPORTES = "http://127.0.0.1:8085";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private KafkaAdminController kafkaAdmin;

    @Autowired
    private ReportesController reportes;

    private MockRestServiceServer servidorKafkaAdmin;
    private MockRestServiceServer servidorReportes;

    @BeforeEach
    void simular() {
        servidorKafkaAdmin = MockRestServiceServer
                .bindTo(AopTestUtils.<KafkaAdminController>getTargetObject(kafkaAdmin).restTemplate).build();
        servidorReportes = MockRestServiceServer
                .bindTo(AopTestUtils.<ReportesController>getTargetObject(reportes).restTemplate).build();
    }

    private static SimpleGrantedAuthority rol(String nombre) {
        return new SimpleGrantedAuthority("ROLE_" + nombre);
    }

    @Test
    @DisplayName("Sin token: 401 en las dos APIs")
    void sinToken() throws Exception {
        mockMvc.perform(get("/api/kafka/topics")).andExpect(status().isUnauthorized());
        mockMvc.perform(get("/api/reportes/pedidos/resumen")).andExpect(status().isUnauthorized());
        servidorKafkaAdmin.verify();
        servidorReportes.verify();
    }

    @Test
    @DisplayName("Cliente: 403. La administracion de Kafka y la auditoria son solo para Admin")
    void clienteNoPuede() throws Exception {
        mockMvc.perform(get("/api/kafka/topics").with(jwt().authorities(rol("Cliente"))))
                .andExpect(status().isForbidden());
        mockMvc.perform(get("/api/reportes/auditoria").with(jwt().authorities(rol("Cliente"))))
                .andExpect(status().isForbidden());
        servidorKafkaAdmin.verify();
        servidorReportes.verify();
    }

    @Test
    @DisplayName("Admin consulta un topico: se reenvia la ruta")
    void adminConsultaTopico() throws Exception {
        servidorKafkaAdmin.expect(requestTo(KAFKAADMIN + "/api/kafka/topics/pedidos-eventos"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"nombre\":\"pedidos-eventos\",\"particiones\":3}",
                        MediaType.APPLICATION_JSON));

        mockMvc.perform(get("/api/kafka/topics/pedidos-eventos").with(jwt().authorities(rol("Admin"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.particiones").value(3));
        servidorKafkaAdmin.verify();
    }

    @Test
    @DisplayName("Admin modifica una configuracion: el cuerpo JSON llega intacto")
    void adminModificaConfiguracion() throws Exception {
        String cuerpo = "{\"configName\":\"retention.ms\",\"value\":\"172800000\"}";
        servidorKafkaAdmin.expect(requestTo(KAFKAADMIN + "/api/kafka/topics/ev3-admin-prueba/configs"))
                .andExpect(method(HttpMethod.PATCH))
                .andExpect(content().json(cuerpo))
                .andRespond(withSuccess("{\"topic\":\"ev3-admin-prueba\"}", MediaType.APPLICATION_JSON));

        mockMvc.perform(patch("/api/kafka/topics/ev3-admin-prueba/configs")
                        .contentType(MediaType.APPLICATION_JSON).content(cuerpo)
                        .with(jwt().authorities(rol("Admin"))))
                .andExpect(status().isOk());
        servidorKafkaAdmin.verify();
    }

    @Test
    @DisplayName("El 409 de kafkaadmin llega intacto")
    void conflictoLlegaIntacto() throws Exception {
        servidorKafkaAdmin.expect(requestTo(KAFKAADMIN + "/api/kafka/topics/pedidos-eventos"))
                .andExpect(method(HttpMethod.DELETE))
                .andRespond(withStatus(HttpStatus.CONFLICT).contentType(MediaType.APPLICATION_JSON)
                        .body("{\"error\":\"El topico pedidos-eventos es parte del caso\"}"));

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .delete("/api/kafka/topics/pedidos-eventos").with(jwt().authorities(rol("Admin"))))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("El topico pedidos-eventos es parte del caso"));
        servidorKafkaAdmin.verify();
    }

    @Test
    @DisplayName("Admin consulta la auditoria de un pedido: se reenvia la query string")
    void adminConsultaAuditoria() throws Exception {
        servidorReportes.expect(requestTo(REPORTES + "/api/reportes/auditoria?pedido=PED-20261110-AB12CD34"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("[{\"accion\":\"CREAR\"}]", MediaType.APPLICATION_JSON));

        mockMvc.perform(get("/api/reportes/auditoria?pedido=PED-20261110-AB12CD34")
                        .with(jwt().authorities(rol("Admin"))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].accion").value("CREAR"));
        servidorReportes.verify();
    }
}
