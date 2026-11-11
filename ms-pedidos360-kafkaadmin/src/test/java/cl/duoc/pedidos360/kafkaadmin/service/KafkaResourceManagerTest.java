package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.web.dto.ConfigTopico;
import cl.duoc.pedidos360.kafkaadmin.web.dto.InfoGrupo;
import cl.duoc.pedidos360.kafkaadmin.web.dto.InfoTopico;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * El administrador contra un Kafka embebido de un broker.
 *
 * Con tres brokers embebidos en una misma JVM el controlador KRaft no se
 * sostiene en estos equipos (NotControllerException: "No controller appears to
 * be active") y las operaciones no terminan. El factor de replicacion 3 y el
 * ISR completo se verifican contra el cluster Docker real, con
 * pruebasverificar-ep3.ps1. Aqui se prueba la logica de la API, y de paso que
 * pedir mas replicas que brokers da un error claro.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafkaadmin.timeout-segundos=30"
})
@EmbeddedKafka(partitions = 3, topics = "pedidos-eventos")
class KafkaResourceManagerTest {

    @Autowired
    private KafkaResourceManager administrador;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private static String nombreDePrueba() {
        return "pedidos-prueba-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    @DisplayName("El cluster informa sus brokers y su controlador")
    void cluster() {
        assertThat(administrador.cluster().brokers()).hasSize(1);
        assertThat(administrador.cluster().controlador()).isNotNull();
    }

    @Test
    @DisplayName("Un topico creado se describe con sus particiones, lider, replicas e ISR")
    void crearYConsultar() {
        String topico = nombreDePrueba();

        administrador.crearTopico(topico, 3, (short) 1, Map.of("retention.ms", "600000"));

        InfoTopico info = administrador.consultarTopico(topico);
        assertThat(info.particiones()).isEqualTo(3);
        assertThat(info.factorReplicacion()).isEqualTo(1);
        assertThat(info.particionesSinIsr()).isZero();
        assertThat(info.detalle()).allSatisfy(p -> {
            assertThat(p.replicas()).hasSize(1);
            assertThat(p.isr()).containsExactlyInAnyOrderElementsOf(p.replicas());
            assertThat(p.replicas()).contains(p.lider());
        });
        assertThat(administrador.listarTopicos(false)).extracting(t -> t.nombre()).contains(topico);
    }

    @Test
    @DisplayName("Pedir mas replicas que brokers vivos lo rechaza el cluster")
    void replicacionMayorQueBrokers() {
        assertThatThrownBy(() -> administrador.crearTopico(nombreDePrueba(), 3, (short) 3, Map.of()))
                .isInstanceOf(KafkaResourceManager.KafkaOperacionException.class)
                .hasCauseInstanceOf(org.apache.kafka.common.errors.InvalidReplicationFactorException.class);
    }

    @Test
    @DisplayName("Crear un topico que ya existe es un conflicto")
    void crearDuplicado() {
        String topico = nombreDePrueba();
        administrador.crearTopico(topico, 1, (short) 1, Map.of());

        assertThatThrownBy(() -> administrador.crearTopico(topico, 1, (short) 1, Map.of()))
                .isInstanceOf(OperacionNoPermitidaException.class)
                .hasMessageContaining("ya existe");
    }

    @Test
    @DisplayName("Modificar retention.ms cambia solo esa propiedad y se comprueba leyendola de nuevo")
    void modificarConfiguracion() {
        String topico = nombreDePrueba();
        administrador.crearTopico(topico, 1, (short) 1, Map.of("cleanup.policy", "delete"));

        ConfigTopico despues = administrador.modificarConfiguracion(topico, Map.of("retention.ms", "300000"));

        assertThat(despues.configuraciones().get("retention.ms").valor()).isEqualTo("300000");
        assertThat(despues.configuraciones().get("retention.ms").origen()).isEqualTo("DYNAMIC_TOPIC_CONFIG");
        assertThat(despues.configuraciones().get("cleanup.policy").valor()).isEqualTo("delete");
    }

    @Test
    @DisplayName("Un topico de prueba se elimina; uno del caso no")
    void eliminar() {
        String topico = nombreDePrueba();
        administrador.crearTopico(topico, 1, (short) 1, Map.of());

        administrador.eliminarTopico(topico);
        assertThatThrownBy(() -> administrador.eliminarTopico("pedidos-eventos"))
                .isInstanceOf(OperacionNoPermitidaException.class);
        assertThat(administrador.consultarTopico("pedidos-eventos").particiones()).isEqualTo(3);
    }

    @Test
    @DisplayName("Consultar algo que no existe responde 'no encontrado', no un error de Kafka")
    void inexistentes() {
        assertThatThrownBy(() -> administrador.consultarTopico("no-existe-" + UUID.randomUUID()))
                .isInstanceOf(RecursoNoEncontradoException.class);
        assertThatThrownBy(() -> administrador.consultarGrupo("grupo-que-no-existe"))
                .isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    @DisplayName("Un consumer group muestra su avance por particion y el lag")
    void consumerGroup() throws Exception {
        String topico = nombreDePrueba();
        administrador.crearTopico(topico, 3, (short) 1, Map.of());

        KafkaTemplate<String, String> productor = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
        for (int i = 0; i < 6; i++) {
            productor.send(topico, "PED-" + i, "evento " + i).get();
        }
        productor.destroy();

        // Un grupo que lee y confirma 0 mensajes: queda con los 6 como lag
        String grupo = "grupo-prueba-" + UUID.randomUUID().toString().substring(0, 8);
        Map<String, Object> propiedades = KafkaTestUtils.consumerProps(grupo, "false", broker);
        propiedades.put("auto.offset.reset", "latest");
        try (Consumer<String, String> consumidor = new DefaultKafkaConsumerFactory<>(
                propiedades, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            consumidor.subscribe(List.of(topico));
            long limite = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (consumidor.assignment().isEmpty() && System.nanoTime() < limite) {
                consumidor.poll(Duration.ofMillis(200));
            }
            // Confirma el comienzo de cada particion: el grupo "no ha procesado nada"
            consumidor.seekToBeginning(consumidor.assignment());
            consumidor.assignment().forEach(p -> consumidor.position(p));
            consumidor.commitSync();

            InfoGrupo activo = administrador.consultarGrupo(grupo);
            assertThat(activo.estado()).isEqualTo("STABLE");
            assertThat(activo.miembros()).hasSize(1);
            assertThat(activo.miembros().get(0).particiones()).hasSize(3);
            assertThat(activo.lagTotal()).isEqualTo(6);
            assertThat(activo.offsets()).hasSize(3)
                    .allSatisfy(o -> assertThat(o.lag()).isEqualTo(o.offsetFinal() - o.offsetConsumido()));

            assertThatThrownBy(() -> administrador.eliminarGrupo(grupo))
                    .isInstanceOf(OperacionNoPermitidaException.class)
                    .hasMessageContaining("consumidores activos");
        }

        assertThat(administrador.listarGrupos()).extracting(g -> g.grupo()).contains(grupo);
        administrador.eliminarGrupo(grupo);
        assertThatThrownBy(() -> administrador.consultarGrupo(grupo)).isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    @DisplayName("Los grupos de ms-pedidos360-reportes estan protegidos")
    void grupoProtegido() {
        assertThatThrownBy(() -> administrador.eliminarGrupo("reportes-pedidos"))
                .isInstanceOf(OperacionNoPermitidaException.class);
    }
}
