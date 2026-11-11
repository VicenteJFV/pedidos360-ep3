package cl.duoc.pedidos360.orders.kafka;

import cl.duoc.pedidos360.orders.client.CatalogClient;
import cl.duoc.pedidos360.orders.client.dto.ProductoCatalogoDto;
import cl.duoc.pedidos360.orders.messaging.PublicadorComandos;
import cl.duoc.pedidos360.orders.service.PedidoService;
import cl.duoc.pedidos360.orders.web.dto.CrearPedidoRequest;
import cl.duoc.pedidos360.orders.web.dto.LineaPedidoRequest;
import cl.duoc.pedidos360.orders.web.dto.PedidoResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * El formato real de los eventos en el broker, contra un Kafka embebido.
 *
 * Lo que importa es el contrato con el consumidor: un JSON con eventId,
 * eventType, occurredAt y data (seccion 5.4 de la guia), con el codigo del
 * pedido como key, y SIN la cabecera __TypeId__ con el nombre de una clase de
 * este proyecto, que obligaria al consumidor a conocerla.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(kraft = true, partitions = 3, topics = {TopicosKafka.EVENTOS, TopicosKafka.AUDITORIA})
class SerializacionEventosKafkaTest {

    @Autowired
    private PedidoService servicio;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @MockBean
    private CatalogClient catalogo;

    @MockBean
    private PublicadorComandos publicadorRabbit;

    private final ObjectMapper json = new ObjectMapper();

    @Test
    @DisplayName("El evento de negocio y el de auditoria llegan al broker como JSON con la estructura de la guia")
    void formatoEnElBroker() throws Exception {
        when(catalogo.obtenerProducto(1L)).thenReturn(new ProductoCatalogoDto(
                1L, "SKU-DISP", "Notebook", new BigDecimal("500000.00"), 50, "COMPUTACION", true));

        PedidoResponse pedido = servicio.crear(new CrearPedidoRequest(
                "cliente@duoc.cl", "Cliente Prueba", "Av. Siempre Viva 742", null,
                List.of(new LineaPedidoRequest(1L, 1))));

        ConsumerRecord<String, String> negocio = leerUno(TopicosKafka.EVENTOS);
        assertThat(negocio.key()).isEqualTo(pedido.codigo());
        assertThat(negocio.headers().lastHeader("__TypeId__")).isNull();
        JsonNode evento = json.readTree(negocio.value());
        assertThat(evento.get("eventType").asText()).isEqualTo("PEDIDO_CREADO");
        assertThat(evento.get("eventId").asText()).isNotBlank();
        assertThat(evento.get("occurredAt").asText()).endsWith("Z");
        assertThat(evento.get("data").get("codigo").asText()).isEqualTo(pedido.codigo());
        assertThat(evento.get("data").get("estado").asText()).isEqualTo("CREADO");

        ConsumerRecord<String, String> auditoria = leerUno(TopicosKafka.AUDITORIA);
        assertThat(auditoria.key()).isEqualTo(pedido.codigo());
        JsonNode registro = json.readTree(auditoria.value());
        assertThat(registro.get("eventType").asText()).isEqualTo("AUDITORIA_PEDIDO");
        assertThat(registro.get("data").get("accion").asText()).isEqualTo("CREAR");
        assertThat(registro.get("data").get("eventoNegocioId").asText())
                .isEqualTo(evento.get("eventId").asText());
    }

    /**
     * Lee asignando las tres particiones a mano, sin consumer group: la prueba
     * no necesita coordinador de grupo, y asi no depende de que el broker
     * embebido haya terminado de crear __consumer_offsets.
     */
    private ConsumerRecord<String, String> leerUno(String topico) {
        Map<String, Object> propiedades = KafkaTestUtils.consumerProps("prueba-" + topico, "false", broker);
        try (Consumer<String, String> consumidor = new DefaultKafkaConsumerFactory<>(
                propiedades, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            List<TopicPartition> particiones = IntStream.range(0, 3)
                    .mapToObj(p -> new TopicPartition(topico, p)).toList();
            consumidor.assign(particiones);
            consumidor.seekToBeginning(particiones);
            return KafkaTestUtils.getSingleRecord(consumidor, topico, Duration.ofSeconds(15));
        }
    }
}
