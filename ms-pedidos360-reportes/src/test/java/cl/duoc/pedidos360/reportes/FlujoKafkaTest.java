package cl.duoc.pedidos360.reportes;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El recorrido completo contra un Kafka embebido: los mismos topicos, los
 * mismos listeners y el mismo KafkaErrorConfig que en produccion.
 *
 * Los mensajes se escriben como texto JSON, igual que los deja
 * ms-pedidos360-orders en el broker: lo que se prueba es el contrato, no una
 * clase compartida.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "reportes.reintentos.intervalo-ms=200"
})
@EmbeddedKafka(partitions = 3, topics = {
        "pedidos-eventos", "pedidos-auditoria", "pedidos-eventos-dlt", "pedidos-auditoria-dlt"})
class FlujoKafkaTest {

    private static final Duration ESPERA = Duration.ofSeconds(40);

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private JdbcTemplate jdbc;

    private KafkaTemplate<String, String> productor;

    @BeforeEach
    void crearProductor() {
        productor = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));
    }

    @AfterEach
    void cerrarProductor() {
        productor.destroy();
    }

    private static String eventoPedido(String eventId, String codigo, String email) {
        return """
                {"eventId":"%s","eventType":"PEDIDO_CREADO","occurredAt":"2026-11-10T18:30:00Z",
                 "data":{"pedidoId":1,"codigo":"%s","estado":"CREADO","clienteEmail":"%s","total":15000.00,"cantidadItems":1}}
                """.formatted(eventId, codigo, email);
    }

    /** Codigo corto como los reales (PED-20261110-AB12CD34 tiene 21 caracteres). */
    private static String codigo(String prefijo) {
        return prefijo + UUID.randomUUID().toString().substring(0, 8);
    }

    private void publicar(String topico, String key, String valor) throws Exception {
        productor.send(topico, key, valor).get();
    }

    @Test
    @DisplayName("Un evento valido de pedidos-eventos llega al reporte")
    void eventoValido() throws Exception {
        String codigo = codigo("PED-OK-");
        publicar("pedidos-eventos", codigo, eventoPedido(UUID.randomUUID().toString(), codigo, "a@duoc.cl"));

        esperar(() -> contar("SELECT COUNT(*) FROM REPORTE_PEDIDOS WHERE CODIGO = ?", codigo) == 1);
        assertThat(jdbc.queryForObject("SELECT ESTADO FROM REPORTE_PEDIDOS WHERE CODIGO = ?", String.class, codigo))
                .isEqualTo("CREADO");
    }

    @Test
    @DisplayName("Un registro de pedidos-auditoria queda con su particion y offset")
    void auditoriaValida() throws Exception {
        String codigo = codigo("PED-AUD-");
        String eventId = UUID.randomUUID().toString();
        publicar("pedidos-auditoria", codigo, """
                {"eventId":"%s","eventType":"AUDITORIA_PEDIDO","occurredAt":"2026-11-10T18:30:00Z",
                 "data":{"accion":"CREAR","codigoPedido":"%s","estadoNuevo":"CREADO",
                         "usuario":"Admin@duoc.cl","origen":"ms-pedidos360-orders"}}
                """.formatted(eventId, codigo));

        esperar(() -> contar("SELECT COUNT(*) FROM AUDITORIA_PEDIDOS WHERE EVENT_ID = ?", eventId) == 1);
        assertThat(jdbc.queryForObject("SELECT OFFSET_KAFKA FROM AUDITORIA_PEDIDOS WHERE EVENT_ID = ?",
                Long.class, eventId)).isNotNull();
    }

    @Test
    @DisplayName("Un mensaje que no es JSON va al DLT con sus bytes originales, sin detener el consumo")
    void mensajeIlegible() throws Exception {
        String key = "ilegible-" + UUID.randomUUID();
        publicar("pedidos-eventos", key, "esto no es json");

        ConsumerRecord<String, byte[]> enDlt = buscarEnDlt("pedidos-eventos-dlt", key);
        assertThat(new String(enDlt.value(), StandardCharsets.UTF_8)).isEqualTo("esto no es json");

        // El consumidor sigue vivo: un evento valido posterior se procesa igual
        String codigo = codigo("PED-DESPUES-");
        publicar("pedidos-eventos", codigo, eventoPedido(UUID.randomUUID().toString(), codigo, "a@duoc.cl"));
        esperar(() -> contar("SELECT COUNT(*) FROM REPORTE_PEDIDOS WHERE CODIGO = ?", codigo) == 1);
    }

    @Test
    @DisplayName("Un evento que no pasa la validacion va al DLT sin reintentos")
    void eventoInvalido() throws Exception {
        String key = "invalido-" + UUID.randomUUID();
        publicar("pedidos-eventos", key, "{\"eventId\":\"x-1\",\"occurredAt\":\"2026-11-10T18:30:00Z\"}");

        ConsumerRecord<String, byte[]> enDlt = buscarEnDlt("pedidos-eventos-dlt", key);
        assertThat(cabecera(enDlt, "kafka_dlt-exception-message")).contains("DTO invalido");
    }

    @Test
    @DisplayName("El fallo controlado agota los reintentos y termina en el DLT con el motivo")
    void falloControlado() throws Exception {
        String codigo = codigo("PED-FALLA-");
        String eventId = UUID.randomUUID().toString();
        publicar("pedidos-eventos", codigo, eventoPedido(eventId, codigo, "error-kafka@pedidos360.cl"));

        ConsumerRecord<String, byte[]> enDlt = buscarEnDlt("pedidos-eventos-dlt", codigo);
        assertThat(new String(enDlt.value(), StandardCharsets.UTF_8)).contains(eventId);
        assertThat(cabecera(enDlt, "kafka_dlt-exception-message")).contains("Fallo controlado");
        assertThat(cabecera(enDlt, "kafka_dlt-original-topic")).isEqualTo("pedidos-eventos");
        assertThat(contar("SELECT COUNT(*) FROM REPORTE_PEDIDOS WHERE CODIGO = ?", codigo)).isZero();
    }

    // -----------------------------------------------------------------------

    private int contar(String sql, String parametro) {
        Integer n = jdbc.queryForObject(sql, Integer.class, parametro);
        return n == null ? 0 : n;
    }

    private static void esperar(BooleanSupplier condicion) throws InterruptedException {
        long limite = System.nanoTime() + ESPERA.toNanos();
        while (System.nanoTime() < limite) {
            if (condicion.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("La condicion no se cumplio en " + ESPERA.toSeconds() + " s");
    }

    private ConsumerRecord<String, byte[]> buscarEnDlt(String topico, String key) {
        Map<String, Object> propiedades = KafkaTestUtils.consumerProps("lector-" + UUID.randomUUID(), "false", broker);
        try (Consumer<String, byte[]> lector = new DefaultKafkaConsumerFactory<>(
                propiedades, new StringDeserializer(), new ByteArrayDeserializer()).createConsumer()) {
            List<TopicPartition> particiones = IntStream.range(0, 3)
                    .mapToObj(p -> new TopicPartition(topico, p)).toList();
            lector.assign(particiones);
            lector.seekToBeginning(particiones);

            long limite = System.nanoTime() + ESPERA.toNanos();
            while (System.nanoTime() < limite) {
                for (ConsumerRecord<String, byte[]> registro : lector.poll(Duration.ofMillis(500))) {
                    if (key.equals(registro.key())) {
                        return registro;
                    }
                }
            }
        }
        throw new AssertionError("No llego al " + topico + " un mensaje con key " + key);
    }

    private static String cabecera(ConsumerRecord<?, ?> registro, String nombre) {
        Header cabecera = registro.headers().lastHeader(nombre);
        return cabecera == null ? null : new String(cabecera.value(), StandardCharsets.UTF_8);
    }
}
