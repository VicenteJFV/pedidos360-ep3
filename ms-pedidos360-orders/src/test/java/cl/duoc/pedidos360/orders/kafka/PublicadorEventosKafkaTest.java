package cl.duoc.pedidos360.orders.kafka;

import cl.duoc.pedidos360.orders.client.CatalogClient;
import cl.duoc.pedidos360.orders.client.dto.ProductoCatalogoDto;
import cl.duoc.pedidos360.orders.exception.ReglaNegocioException;
import cl.duoc.pedidos360.orders.messaging.PublicadorComandos;
import cl.duoc.pedidos360.orders.service.PedidoService;
import cl.duoc.pedidos360.orders.web.dto.CrearPedidoRequest;
import cl.duoc.pedidos360.orders.web.dto.LineaPedidoRequest;
import cl.duoc.pedidos360.orders.web.dto.PedidoResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Lo que efectivamente sale hacia Kafka y cuando.
 *
 * Estas pruebas NO son @Transactional a proposito: cada operacion del servicio
 * hace commit de verdad, que es el momento en que PublicadorEventosKafka
 * publica. Con @Transactional el commit nunca ocurre y no se veria nada.
 *
 * El KafkaTemplate se simula: el formato real en el broker se prueba aparte,
 * en SerializacionEventosKafkaTest, contra un Kafka embebido.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class PublicadorEventosKafkaTest {

    private static final Long PRODUCTO = 1L;

    @Autowired
    private PedidoService servicio;

    @MockBean
    private CatalogClient catalogo;

    @MockBean
    private PublicadorComandos publicadorRabbit;

    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach
    void preparar() {
        when(catalogo.obtenerProducto(PRODUCTO)).thenReturn(new ProductoCatalogoDto(
                PRODUCTO, "SKU-DISP", "Notebook", new BigDecimal("500000.00"), 50, "COMPUTACION", true));
        // Un futuro que no termina: el callback del resultado no corre, y la
        // prueba se concentra en que se envio, a donde y con que contenido.
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(new CompletableFuture<>());
    }

    private PedidoResponse crearPedido() {
        return servicio.crear(new CrearPedidoRequest(
                "cliente@duoc.cl", "Cliente Prueba", "Av. Siempre Viva 742", null,
                List.of(new LineaPedidoRequest(PRODUCTO, 2))));
    }

    @Test
    @DisplayName("Cada hecho publica un evento de negocio y uno de auditoria, con el codigo como key")
    void unHechoDosTopicos() {
        PedidoResponse pedido = crearPedido();

        ArgumentCaptor<Object> negocio = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(TopicosKafka.EVENTOS), eq(pedido.codigo()), negocio.capture());
        ArgumentCaptor<Object> auditoria = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(TopicosKafka.AUDITORIA), eq(pedido.codigo()), auditoria.capture());

        EventoPedido evento = (EventoPedido) negocio.getValue();
        assertThat(evento.eventType()).isEqualTo("PEDIDO_CREADO");
        assertThat(evento.eventId()).isNotBlank();
        assertThat(evento.occurredAt()).endsWith("Z");
        assertThat(evento.data().codigo()).isEqualTo(pedido.codigo());
        assertThat(evento.data().estado()).isEqualTo("CREADO");
        assertThat(evento.data().estadoAnterior()).isNull();
        assertThat(evento.data().total()).isEqualByComparingTo("1000000.00");

        EventoAuditoria registro = (EventoAuditoria) auditoria.getValue();
        assertThat(registro.eventType()).isEqualTo("AUDITORIA_PEDIDO");
        assertThat(registro.data().accion()).isEqualTo("CREAR");
        assertThat(registro.data().usuario()).isEqualTo("sistema");
        assertThat(registro.data().origen()).isEqualTo("ms-pedidos360-orders");
        // Los dos eventos del mismo hecho quedan enlazados
        assertThat(registro.data().eventoNegocioId()).isEqualTo(evento.eventId());
        assertThat(registro.eventId()).isNotEqualTo(evento.eventId());
    }

    @Test
    @DisplayName("Aceptar publica PEDIDO_ACEPTADO con el estado anterior")
    void aceptarPublica() {
        PedidoResponse pedido = crearPedido();
        clearInvocations(kafkaTemplate);

        servicio.aceptar(pedido.id());

        ArgumentCaptor<Object> negocio = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq(TopicosKafka.EVENTOS), eq(pedido.codigo()), negocio.capture());
        EventoPedido evento = (EventoPedido) negocio.getValue();
        assertThat(evento.eventType()).isEqualTo("PEDIDO_ACEPTADO");
        assertThat(evento.data().estadoAnterior()).isEqualTo("CREADO");
        verify(kafkaTemplate, times(1)).send(eq(TopicosKafka.AUDITORIA), eq(pedido.codigo()), any());
    }

    @Test
    @DisplayName("Si la transaccion se revierte, no sale nada hacia Kafka")
    void rollbackNoPublica() {
        PedidoResponse pedido = crearPedido();
        clearInvocations(kafkaTemplate);
        doThrow(new ReglaNegocioException("Stock insuficiente"))
                .when(catalogo).reservarStock(anyString(), any());

        assertThatThrownBy(() -> servicio.aceptar(pedido.id())).isInstanceOf(ReglaNegocioException.class);

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("Si Kafka no responde, el pedido igual queda confirmado")
    void kafkaCaidoNoRompeElNegocio() {
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new org.apache.kafka.common.KafkaException("Topic not present in metadata after 5000 ms"));

        PedidoResponse pedido = crearPedido();

        assertThat(servicio.obtener(pedido.id()).codigo()).isEqualTo(pedido.codigo());
    }
}
