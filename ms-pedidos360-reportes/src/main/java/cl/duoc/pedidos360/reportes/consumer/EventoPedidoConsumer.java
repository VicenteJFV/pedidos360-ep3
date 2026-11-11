package cl.duoc.pedidos360.reportes.consumer;

import cl.duoc.pedidos360.reportes.model.EventoPedido;
import cl.duoc.pedidos360.reportes.service.ReporteService;
import cl.duoc.pedidos360.reportes.service.Resultado;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Consumidor de pedidos-eventos: alimenta la reporteria.
 *
 * Mismo esquema que el EventoConsumer de la guia practica 3.3.2: recibe el DTO,
 * lo valida y lo procesa; si algo falla lanza una excepcion y el
 * DefaultErrorHandler de KafkaErrorConfig decide si reintentar o mandarlo al
 * DLT. El listener no confirma offsets a mano: el contenedor confirma el offset
 * cuando el metodo termina sin excepcion, o cuando el mensaje ya quedo en el DLT.
 *
 * La logica de negocio vive en ReporteService; aqui solo hay mensajeria.
 */
@Service
public class EventoPedidoConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventoPedidoConsumer.class);

    static final String TOPICO = "pedidos-eventos";
    static final String GRUPO = "reportes-pedidos";

    private final ValidadorEventos validador;
    private final ReporteService reportes;

    public EventoPedidoConsumer(ValidadorEventos validador, ReporteService reportes) {
        this.validador = validador;
        this.reportes = reportes;
    }

    @KafkaListener(
            id = GRUPO,
            topics = TOPICO,
            groupId = GRUPO,
            // El tipo del JSON de este topico. Cada listener fija el suyo porque
            // el productor no envia la cabecera __TypeId__.
            properties = "spring.json.value.default.type=cl.duoc.pedidos360.reportes.model.EventoPedido")
    public void procesar(
            @Payload(required = false) EventoPedido evento,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("EVENTO RECIBIDO | Topico: {} | Particion: {} | Offset: {} | Key: {} | eventType: {} | eventId: {}",
                TOPICO, particion, offset, key,
                evento == null ? null : evento.eventType(), evento == null ? null : evento.eventId());

        // 1. Validar el contenido antes de procesarlo
        Instant ocurrido;
        try {
            ocurrido = validador.validar(evento, evento == null ? null : evento.occurredAt());
        } catch (EventoInvalidoException e) {
            log.error("EVENTO INVALIDO | Topico: {} | Particion: {} | Offset: {} | Key: {} | motivo: {} | no se reintenta",
                    TOPICO, particion, offset, key, e.getMessage());
            throw e;
        }

        // 2. Procesar. Un fallo aqui se reintenta (ver KafkaErrorConfig).
        Resultado resultado = reportes.registrar(evento, ocurrido);

        switch (resultado) {
            case PROCESADO -> log.info(
                    "EVENTO PROCESADO | Topico: {} | eventType: {} | eventId: {} | pedido: {} | estado: {}",
                    TOPICO, evento.eventType(), evento.eventId(), evento.data().codigo(), evento.data().estado());
            case DUPLICADO -> log.info(
                    "EVENTO DUPLICADO IGNORADO | Topico: {} | eventId: {} | ya estaba en PROCESSED_MESSAGES",
                    TOPICO, evento.eventId());
            case ANTERIOR_AL_ESTADO_ACTUAL -> log.warn(
                    "EVENTO ANTERIOR AL ESTADO ACTUAL | Topico: {} | eventId: {} | pedido: {} | el reporte no retrocede",
                    TOPICO, evento.eventId(), evento.data().codigo());
        }
    }
}
