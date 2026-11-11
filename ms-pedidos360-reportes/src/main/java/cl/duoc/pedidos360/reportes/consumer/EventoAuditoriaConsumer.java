package cl.duoc.pedidos360.reportes.consumer;

import cl.duoc.pedidos360.reportes.model.EventoAuditoria;
import cl.duoc.pedidos360.reportes.service.AuditoriaService;
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
 * Consumidor de pedidos-auditoria: guarda la historia de acciones de cada pedido.
 *
 * Su consumer group es distinto del de la reporteria (reportes-auditoria contra
 * reportes-pedidos): cada grupo lleva sus propios offsets, y el lag de un
 * topico no se mezcla con el del otro.
 */
@Service
public class EventoAuditoriaConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventoAuditoriaConsumer.class);

    static final String TOPICO = "pedidos-auditoria";
    static final String GRUPO = "reportes-auditoria";

    private final ValidadorEventos validador;
    private final AuditoriaService auditoria;

    public EventoAuditoriaConsumer(ValidadorEventos validador, AuditoriaService auditoria) {
        this.validador = validador;
        this.auditoria = auditoria;
    }

    @KafkaListener(
            id = GRUPO,
            topics = TOPICO,
            groupId = GRUPO,
            properties = "spring.json.value.default.type=cl.duoc.pedidos360.reportes.model.EventoAuditoria")
    public void procesar(
            @Payload(required = false) EventoAuditoria registro,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int particion,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        log.info("EVENTO RECIBIDO | Topico: {} | Particion: {} | Offset: {} | Key: {} | eventType: {} | eventId: {}",
                TOPICO, particion, offset, key,
                registro == null ? null : registro.eventType(), registro == null ? null : registro.eventId());

        Instant ocurrido;
        try {
            ocurrido = validador.validar(registro, registro == null ? null : registro.occurredAt());
        } catch (EventoInvalidoException e) {
            log.error("EVENTO INVALIDO | Topico: {} | Particion: {} | Offset: {} | Key: {} | motivo: {} | no se reintenta",
                    TOPICO, particion, offset, key, e.getMessage());
            throw e;
        }

        Resultado resultado = auditoria.registrar(registro, ocurrido, particion, offset);

        if (resultado == Resultado.DUPLICADO) {
            log.info("EVENTO DUPLICADO IGNORADO | Topico: {} | eventId: {} | ya estaba en PROCESSED_MESSAGES",
                    TOPICO, registro.eventId());
        } else {
            log.info("EVENTO PROCESADO | Topico: {} | accion: {} | usuario: {} | pedido: {} | {} -> {}",
                    TOPICO, registro.data().accion(), registro.data().usuario(), registro.data().codigoPedido(),
                    registro.data().estadoAnterior(), registro.data().estadoNuevo());
        }
    }
}
