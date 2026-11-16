package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.dto.ConsumerGroupDetailResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.CreateTopicRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicDescriptionResponse;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
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
 * Las pruebas funcionales de la guia 3.4.1, contra un Kafka embebido de un broker.
 *
 * Con tres brokers embebidos en una misma JVM el controlador KRaft no se
 * sostiene en estos equipos (NotControllerException: "No controller appears to
 * be active") y las operaciones no terminan. El factor de replicacion 3 y el
 * ISR completo se verifican contra el cluster Docker real. Aqui se prueba la
 * logica de la API, y de paso que pedir mas replicas que brokers da un error
 * claro.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "kafka.admin.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafkaadmin.timeout-segundos=30"
})
@EmbeddedKafka(partitions = 3, topics = "pedidos-eventos")
class KafkaAdminServiceTest {

    @Autowired
    private KafkaAdminService service;

    @Autowired
    private EmbeddedKafkaBroker broker;

    private static String nombreDePrueba() {
        return "ev3-admin-prueba-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private void crear(String nombre, int particiones, Map<String, String> configs) {
        service.createTopic(new CreateTopicRequest(nombre, particiones, (short) 1, configs));
    }

    @Test
    @DisplayName("El cluster informa sus brokers y su controlador")
    void cluster() {
        assertThat(service.cluster().brokers()).hasSize(1);
        assertThat(service.cluster().controller()).isNotNull();
    }

    @Test
    @DisplayName("Pruebas 1 a 3: crear, listar y describir con particiones, lider, replicas e ISR")
    void crearListarDescribir() {
        String topico = nombreDePrueba();

        crear(topico, 3, Map.of("retention.ms", "86400000", "cleanup.policy", "delete"));

        assertThat(service.listTopics()).contains(topico, "pedidos-eventos");
        TopicDescriptionResponse info = service.describeTopic(topico);
        assertThat(info.partitions()).hasSize(3);
        assertThat(info.replicationFactor()).isEqualTo(1);
        assertThat(info.underReplicated()).isZero();
        assertThat(info.partitions()).allSatisfy(p -> {
            assertThat(p.isr()).containsExactlyInAnyOrderElementsOf(p.replicas());
            assertThat(p.replicas()).contains(p.leader());
        });
    }

    @Test
    @DisplayName("Pedir mas replicas que brokers vivos lo rechaza el cluster")
    void replicacionMayorQueBrokers() {
        assertThatThrownBy(() -> service.createTopic(new CreateTopicRequest(nombreDePrueba(), 3, (short) 3, Map.of())))
                .isInstanceOf(KafkaAdminService.KafkaOperacionException.class)
                .hasCauseInstanceOf(org.apache.kafka.common.errors.InvalidReplicationFactorException.class);
    }

    @Test
    @DisplayName("Crear un topico que ya existe es un conflicto")
    void crearDuplicado() {
        String topico = nombreDePrueba();
        crear(topico, 1, Map.of());

        assertThatThrownBy(() -> crear(topico, 1, Map.of()))
                .isInstanceOf(OperacionNoPermitidaException.class)
                .hasMessageContaining("ya existe");
    }

    @Test
    @DisplayName("Una configuracion fuera de rango se rechaza antes de llegar al cluster")
    void configuracionInvalidaAlCrear() {
        assertThatThrownBy(() -> crear(nombreDePrueba(), 1, Map.of("cleanup.policy", "borrar")))
                .isInstanceOf(SolicitudInvalidaException.class)
                .extracting(e -> ((SolicitudInvalidaException) e).getParametro())
                .isEqualTo("configs.cleanup.policy");
    }

    @Test
    @DisplayName("Pruebas 4 y 5: consultar y modificar retention.ms, y comprobarlo despues")
    void modificarConfiguracion() {
        String topico = nombreDePrueba();
        crear(topico, 1, Map.of("retention.ms", "86400000", "cleanup.policy", "delete"));
        assertThat(service.topicConfigs(topico)).containsEntry("retention.ms", "86400000");

        service.updateConfig(topico, "retention.ms", "172800000");

        assertThat(service.topicConfigs(topico))
                .containsEntry("retention.ms", "172800000")
                .containsEntry("cleanup.policy", "delete");
    }

    @Test
    @DisplayName("Prueba 6: aumentar de 3 a 4 particiones; reducir no se permite")
    void aumentarParticiones() throws InterruptedException {
        String topico = nombreDePrueba();
        crear(topico, 3, Map.of());

        service.increasePartitions(topico, 4);

        // Como advierte la guia, el cambio puede tardar unos segundos en verse
        long limite = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (service.describeTopic(topico).partitions().size() < 4 && System.nanoTime() < limite) {
            Thread.sleep(200);
        }
        assertThat(service.describeTopic(topico).partitions()).hasSize(4);

        assertThatThrownBy(() -> service.increasePartitions(topico, 2))
                .isInstanceOf(SolicitudInvalidaException.class)
                .hasMessageContaining("mayor que el numero actual de particiones (4)");
    }

    @Test
    @DisplayName("Un topico del caso no se elimina")
    void eliminarProtegido() {
        assertThatThrownBy(() -> service.deleteTopic("pedidos-eventos"))
                .isInstanceOf(OperacionNoPermitidaException.class);
        assertThat(service.describeTopic("pedidos-eventos").partitions()).hasSize(3);
    }

    /**
     * No corre en Windows: al eliminar un topico, Kafka renombra su carpeta de
     * datos mientras Windows la mantiene abierta, el renombre falla y el broker
     * embebido se apaga entero (KAFKA-1194), arrastrando a las demas pruebas. En
     * Linux -el cluster Docker y la EC2- no ocurre; ahi se verifica la Prueba 8.
     */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    @DisplayName("Prueba 8: un topico de prueba se elimina")
    void eliminarTopicoDePrueba() {
        String topico = nombreDePrueba();
        crear(topico, 1, Map.of());

        service.deleteTopic(topico);

        assertThatThrownBy(() -> service.describeTopic(topico)).isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    @DisplayName("Consultar algo que no existe responde 'no encontrado', no un error de Kafka")
    void inexistentes() {
        assertThatThrownBy(() -> service.describeTopic("no-existe-" + UUID.randomUUID()))
                .isInstanceOf(RecursoNoEncontradoException.class);
        assertThatThrownBy(() -> service.consumerGroup("grupo-que-no-existe"))
                .isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    @DisplayName("Prueba 7 y seccion 6.2 de EP3: un grupo muestra miembros, offsets y lag")
    void consumerGroup() throws Exception {
        String topico = nombreDePrueba();
        crear(topico, 3, Map.of());

        KafkaTemplate<String, String> productor = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
        for (int i = 0; i < 6; i++) {
            productor.send(topico, "PED-" + i, "evento " + i).get();
        }
        productor.destroy();

        // Un grupo que confirma el comienzo de cada particion: queda con los 6 como lag
        String grupo = "grupo-prueba-" + UUID.randomUUID().toString().substring(0, 8);
        Map<String, Object> propiedades = KafkaTestUtils.consumerProps(broker, grupo, false);
        propiedades.put("auto.offset.reset", "latest");
        try (Consumer<String, String> consumidor = new DefaultKafkaConsumerFactory<>(
                propiedades, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            consumidor.subscribe(List.of(topico));
            long limite = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (consumidor.assignment().isEmpty() && System.nanoTime() < limite) {
                consumidor.poll(Duration.ofMillis(200));
            }
            consumidor.seekToBeginning(consumidor.assignment());
            consumidor.assignment().forEach(consumidor::position);
            consumidor.commitSync();

            ConsumerGroupDetailResponse activo = service.consumerGroup(grupo);
            assertThat(activo.state()).isEqualTo("STABLE");
            assertThat(activo.members()).hasSize(1);
            assertThat(activo.members().get(0).partitions()).hasSize(3);
            assertThat(activo.totalLag()).isEqualTo(6);
            assertThat(activo.offsets()).hasSize(3)
                    .allSatisfy(o -> assertThat(o.lag()).isEqualTo(o.endOffset() - o.committedOffset()));

            assertThatThrownBy(() -> service.deleteConsumerGroup(grupo))
                    .isInstanceOf(OperacionNoPermitidaException.class)
                    .hasMessageContaining("consumidores activos");
        }

        assertThat(service.consumerGroups()).extracting(g -> g.groupId()).contains(grupo);
        service.deleteConsumerGroup(grupo);
        assertThatThrownBy(() -> service.consumerGroup(grupo)).isInstanceOf(RecursoNoEncontradoException.class);
    }

    @Test
    @DisplayName("Los grupos de ms-pedidos360-reportes estan protegidos")
    void grupoProtegido() {
        assertThatThrownBy(() -> service.deleteConsumerGroup("reportes-pedidos"))
                .isInstanceOf(OperacionNoPermitidaException.class);
    }
}
