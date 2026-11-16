package cl.duoc.pedidos360.kafkaadmin.controller;

import cl.duoc.pedidos360.kafkaadmin.dto.CreateTopicRequest;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;
import cl.duoc.pedidos360.kafkaadmin.service.OperacionNoPermitidaException;
import cl.duoc.pedidos360.kafkaadmin.service.RecursoNoEncontradoException;
import cl.duoc.pedidos360.kafkaadmin.service.SolicitudInvalidaException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Los endpoints de la guia 3.4.1 y sus respuestas de error, sin cluster: el
 * servicio se simula. Los 400 por DTO invalido se rechazan ANTES de llamar al
 * servicio (verifyNoInteractions).
 */
@WebMvcTest(KafkaAdminController.class)
class KafkaAdminControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private KafkaAdminService service;

    private static final String TOPICO_PRUEBA = """
            {"name":"ev3-admin-prueba","partitions":3,"replicationFactor":3,
             "configs":{"retention.ms":"86400000","cleanup.policy":"delete"}}
            """;

    @Test
    @DisplayName("Prueba 2 de la guia: crear el topico de prueba responde 201")
    void crear() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON).content(TOPICO_PRUEBA))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.message").value("Topic creado"))
                .andExpect(jsonPath("$.topic").value("ev3-admin-prueba"));

        verify(service).createTopic(new CreateTopicRequest("ev3-admin-prueba", 3, (short) 3,
                Map.of("retention.ms", "86400000", "cleanup.policy", "delete")));
    }

    @Test
    @DisplayName("Sin nombre: 400 que nombra el campo, sin tocar el cluster")
    void crearSinNombre() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("name"));
        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("Un nombre que Kafka no acepta, o con el prefijo de los internos: 400")
    void crearNombreInvalido() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"pedidos eventos\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("name"));
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"__consumer_offsets\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("Sin particiones o con 0: 400")
    void particionesInvalidas() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"t\",\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("partitions"));
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"t\",\"partitions\":0,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("partitions"));
        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("Factor de replicacion mayor que los 3 brokers: 400")
    void replicacionInvalida() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"t\",\"partitions\":3,\"replicationFactor\":4}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("replicationFactor"));
        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("Un cuerpo que no es JSON: 400")
    void cuerpoIlegible() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON).content("name=t"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("body"));
        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("Prueba 5 de la guia: modificar retention.ms")
    void modificarConfiguracion() throws Exception {
        mvc.perform(patch("/api/kafka/topics/ev3-admin-prueba/configs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configName\":\"retention.ms\",\"value\":\"172800000\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.config").value("retention.ms"))
                .andExpect(jsonPath("$.value").value("172800000"));

        verify(service).updateConfig("ev3-admin-prueba", "retention.ms", "172800000");
    }

    @Test
    @DisplayName("Modificar sin configName: 400 sin tocar el cluster")
    void modificarSinNombre() throws Exception {
        mvc.perform(patch("/api/kafka/topics/ev3-admin-prueba/configs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"value\":\"172800000\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("configName"));
        verifyNoInteractions(service);
    }

    @Test
    @DisplayName("Un valor fuera de rango que detecta el servicio llega como 400 con el campo")
    void valorFueraDeRango() throws Exception {
        doThrow(new SolicitudInvalidaException("retention.ms debe ser un entero mayor que 0", "value"))
                .when(service).updateConfig("ev3-admin-prueba", "retention.ms", "-5");

        mvc.perform(patch("/api/kafka/topics/ev3-admin-prueba/configs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"configName\":\"retention.ms\",\"value\":\"-5\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("value"));
    }

    @Test
    @DisplayName("Prueba 6 de la guia: aumentar particiones")
    void aumentarParticiones() throws Exception {
        mvc.perform(patch("/api/kafka/topics/ev3-admin-prueba/partitions").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"totalPartitions\":4}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalPartitions").value("4"));

        verify(service).increasePartitions("ev3-admin-prueba", 4);
    }

    @Test
    @DisplayName("Eliminar un topico del caso: 409")
    void eliminarProtegido() throws Exception {
        doThrow(new OperacionNoPermitidaException("El topico pedidos-eventos es parte del caso"))
                .when(service).deleteTopic("pedidos-eventos");

        mvc.perform(delete("/api/kafka/topics/pedidos-eventos"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("El topico pedidos-eventos es parte del caso"));
    }

    @Test
    @DisplayName("Consultar un topico que no existe: 404 y no 500")
    void consultarInexistente() throws Exception {
        when(service.describeTopic("no-existe"))
                .thenThrow(new RecursoNoEncontradoException("No existe el topico no-existe"));

        mvc.perform(get("/api/kafka/topics/no-existe"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("No existe el topico no-existe"));
    }

    @Test
    @DisplayName("Lo que rechaza el cluster por invalido llega como 400")
    void rechazoDelCluster() throws Exception {
        doThrow(new KafkaAdminService.KafkaOperacionException(
                new InvalidReplicationFactorException("Replication factor: 3 larger than available brokers: 2.")))
                .when(service).createTopic(any());

        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON).content(TOPICO_PRUEBA))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString("larger than available brokers")));
    }
}
