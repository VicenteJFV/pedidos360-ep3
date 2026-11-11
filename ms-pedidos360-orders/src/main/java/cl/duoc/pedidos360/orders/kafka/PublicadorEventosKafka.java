package cl.duoc.pedidos360.orders.kafka;

import cl.duoc.pedidos360.orders.service.CambioEstadoPedido;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.UUID;

/**
 * Productor Kafka de Pedidos360: el unico del caso (seccion 4.3 de la guia).
 *
 * Por cada hecho del pedido publica dos eventos con el mismo KafkaTemplate:
 *
 *   pedidos-eventos    EventoPedido     que le paso al pedido (PEDIDO_ACEPTADO...)
 *   pedidos-auditoria  EventoAuditoria  quien lo hizo y desde donde
 *
 * La key de ambos es el codigo del pedido. Kafka garantiza el orden dentro de
 * una particion, y una misma key cae siempre en la misma particion: asi los
 * eventos de un pedido llegan al consumidor en el orden en que ocurrieron
 * (CREADO antes que ACEPTADO), aunque el topico tenga tres particiones.
 *
 * Publica DESPUES del commit. Si la transaccion de negocio se revierte -por
 * ejemplo, el catalogo rechaza la reserva de stock- no se emite nada: Kafka
 * nunca registra un hecho que no ocurrio.
 *
 * Kafka complementa a RabbitMQ, no lo reemplaza (seccion 8 de la guia):
 * RabbitMQ sigue llevando las ordenes de trabajo (enviar el correo, imprimir el
 * ticket), y Kafka conserva el historial de hechos para reporteria y auditoria.
 */
@Component
public class PublicadorEventosKafka {

    private static final Logger log = LoggerFactory.getLogger(PublicadorEventosKafka.class);

    static final String ORIGEN = "ms-pedidos360-orders";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PublicadorEventosKafka(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void alConfirmarseElCambio(CambioEstadoPedido cambio) {
        EventoPedido evento = eventoDeNegocio(cambio);
        publicar(TopicosKafka.EVENTOS, cambio.codigo(), evento.eventType(), evento.eventId(), evento);

        EventoAuditoria auditoria = eventoDeAuditoria(cambio, evento.eventId());
        publicar(TopicosKafka.AUDITORIA, cambio.codigo(), auditoria.eventType(), auditoria.eventId(), auditoria);
    }

    static EventoPedido eventoDeNegocio(CambioEstadoPedido cambio) {
        return new EventoPedido(
                UUID.randomUUID().toString(),
                "PEDIDO_" + cambio.estadoNuevo().name(),
                cambio.ocurridoEn().toString(),
                new EventoPedido.Datos(
                        cambio.pedidoId(),
                        cambio.codigo(),
                        cambio.estadoNuevo().name(),
                        cambio.estadoAnterior() == null ? null : cambio.estadoAnterior().name(),
                        cambio.clienteEmail(),
                        cambio.total(),
                        cambio.cantidadItems()));
    }

    static EventoAuditoria eventoDeAuditoria(CambioEstadoPedido cambio, String eventoNegocioId) {
        return new EventoAuditoria(
                UUID.randomUUID().toString(),
                EventoAuditoria.TIPO,
                cambio.ocurridoEn().toString(),
                new EventoAuditoria.Datos(
                        cambio.accion(),
                        cambio.codigo(),
                        cambio.estadoAnterior() == null ? null : cambio.estadoAnterior().name(),
                        cambio.estadoNuevo().name(),
                        cambio.usuario(),
                        ORIGEN,
                        cambio.motivo(),
                        eventoNegocioId));
    }

    /**
     * El resultado de send() llega despues, en otro hilo: recien ahi se sabe en
     * que particion y offset quedo el evento, o por que fallo.
     *
     * Un fallo de Kafka no revierte el pedido, que ya esta confirmado en Oracle:
     * se registra con el topico, la key y el eventId para poder identificarlo.
     * Garantizar la entrega pese a una caida de Kafka exigiria un outbox
     * transaccional; queda fuera del alcance de esta entrega.
     */
    private void publicar(String topico, String key, String tipo, String eventId, Object evento) {
        log.info("Publicando evento Kafka | Topico: {} | Key: {} | eventType: {} | eventId: {}",
                topico, key, tipo, eventId);
        try {
            kafkaTemplate.send(topico, key, evento).whenComplete((resultado, error) -> {
                if (error != null) {
                    log.error("ERROR AL PUBLICAR EN KAFKA | Topico: {} | Key: {} | eventType: {} | eventId: {} | motivo: {}",
                            topico, key, tipo, eventId, error.getMessage());
                    return;
                }
                RecordMetadata metadatos = resultado.getRecordMetadata();
                log.info("EVENTO KAFKA PUBLICADO | Topico: {} | Particion: {} | Offset: {} | Key: {} | eventType: {} | eventId: {}",
                        metadatos.topic(), metadatos.partition(), metadatos.offset(), key, tipo, eventId);
            });
        } catch (RuntimeException e) {
            // send() lanza de inmediato si no obtiene los metadatos del topico en
            // max.block.ms, por ejemplo con el cluster caido.
            log.error("ERROR AL PUBLICAR EN KAFKA | Topico: {} | Key: {} | eventType: {} | eventId: {} | motivo: {}",
                    topico, key, tipo, eventId, e.getMessage());
        }
    }
}
