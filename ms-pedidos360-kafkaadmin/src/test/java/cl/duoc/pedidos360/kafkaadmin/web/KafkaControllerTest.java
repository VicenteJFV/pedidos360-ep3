package cl.duoc.pedidos360.kafkaadmin.web;

import cl.duoc.pedidos360.kafkaadmin.service.KafkaResourceManager;
import cl.duoc.pedidos360.kafkaadmin.service.OperacionNoPermitidaException;
import cl.duoc.pedidos360.kafkaadmin.service.RecursoNoEncontradoException;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Validacion y codigos HTTP del administrador, sin cluster: el servicio se
 * simula. Lo importante de los 400 es que se rechazan ANTES de llamar al
 * servicio (verifyNoInteractions).
 */
@WebMvcTest(KafkaController.class)
class KafkaControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private KafkaResourceManager administrador;

    @Test
    @DisplayName("Crear un topico valido responde 201")
    void crearValido() throws Exception {
        mvc.perform(post("/api/kafka/topics?name=pedidos-prueba&partitions=3&replicationFactor=3"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.operacion").value("crear"))
                .andExpect(jsonPath("$.nombre").value("pedidos-prueba"));

        verify(administrador).crearTopico(eq("pedidos-prueba"), eq(3), eq((short) 3), eq(Map.of()));
    }

    @Test
    @DisplayName("Sin nombre: 400 que nombra el parametro, sin tocar el cluster")
    void crearSinNombre() throws Exception {
        mvc.perform(post("/api/kafka/topics?name=&partitions=3&replicationFactor=3"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("name"));
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Un nombre con caracteres que Kafka no acepta: 400")
    void crearNombreInvalido() throws Exception {
        mvc.perform(post("/api/kafka/topics?name=pedidos eventos&partitions=3&replicationFactor=3"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("name"));
        mvc.perform(post("/api/kafka/topics?name=__consumer_offsets&partitions=3&replicationFactor=3"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Particiones fuera de rango o no numericas: 400")
    void particionesInvalidas() throws Exception {
        mvc.perform(post("/api/kafka/topics?name=t&partitions=0&replicationFactor=3"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("partitions"));
        mvc.perform(post("/api/kafka/topics?name=t&partitions=tres&replicationFactor=3"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("partitions"));
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Factor de replicacion mayor que los brokers del cluster: 400")
    void replicacionInvalida() throws Exception {
        mvc.perform(post("/api/kafka/topics?name=t&partitions=3&replicationFactor=4"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("replicationFactor"));
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Una politica de limpieza que no existe: 400 con el nombre del parametro de la peticion")
    void cleanupPolicyInvalida() throws Exception {
        mvc.perform(post("/api/kafka/topics?name=t&partitions=3&replicationFactor=3&cleanupPolicy=borrar"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("cleanupPolicy"));
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Modificar una configuracion no administrable: 400")
    void configuracionNoAdministrable() throws Exception {
        mvc.perform(put("/api/kafka/topics/pedidos-eventos/config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"compression.type\": \"gzip\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("compression.type"));
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Modificar con un valor fuera de rango: 400")
    void configuracionFueraDeRango() throws Exception {
        mvc.perform(put("/api/kafka/topics/pedidos-eventos/config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"retention.ms\": \"-5\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("retention.ms"));
        mvc.perform(put("/api/kafka/topics/pedidos-eventos/config")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"min.insync.replicas\": 4}"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Modificar sin cuerpo o con un cuerpo que no es JSON: 400")
    void configuracionSinCuerpo() throws Exception {
        mvc.perform(put("/api/kafka/topics/pedidos-eventos/config").contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
        mvc.perform(put("/api/kafka/topics/pedidos-eventos/config")
                        .contentType(MediaType.APPLICATION_JSON).content("retention=1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.parametro").value("body"));
        verifyNoInteractions(administrador);
    }

    @Test
    @DisplayName("Eliminar un topico del caso: 409")
    void eliminarProtegido() throws Exception {
        doThrow(new OperacionNoPermitidaException("El topico pedidos-eventos es parte del caso"))
                .when(administrador).eliminarTopico("pedidos-eventos");

        mvc.perform(delete("/api/kafka/topics/pedidos-eventos"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("El topico pedidos-eventos es parte del caso"));
    }

    @Test
    @DisplayName("Consultar un topico que no existe: 404 y no 500")
    void consultarInexistente() throws Exception {
        when(administrador.consultarTopico("no-existe"))
                .thenThrow(new RecursoNoEncontradoException("No existe el topico no-existe"));

        mvc.perform(get("/api/kafka/topics/no-existe"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("No existe el topico no-existe"));
    }

    @Test
    @DisplayName("Lo que rechaza el cluster por invalido llega como 400")
    void rechazoDelCluster() throws Exception {
        doThrow(new KafkaResourceManager.KafkaOperacionException(
                new InvalidReplicationFactorException("Replication factor: 3 larger than available brokers: 2.")))
                .when(administrador).crearTopico(anyString(), anyInt(), anyShort(), any());

        mvc.perform(post("/api/kafka/topics?name=t&partitions=3&replicationFactor=3"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value(org.hamcrest.Matchers.containsString("larger than available brokers")));
    }
}
