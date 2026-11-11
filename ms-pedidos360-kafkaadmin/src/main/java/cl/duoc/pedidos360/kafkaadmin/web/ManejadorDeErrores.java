package cl.duoc.pedidos360.kafkaadmin.web;

import cl.duoc.pedidos360.kafkaadmin.service.KafkaResourceManager.KafkaOperacionException;
import cl.duoc.pedidos360.kafkaadmin.service.OperacionNoPermitidaException;
import cl.duoc.pedidos360.kafkaadmin.service.RecursoNoEncontradoException;
import cl.duoc.pedidos360.kafkaadmin.web.dto.RespuestaError;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.concurrent.TimeoutException;

/**
 * Traduce las fallas a respuestas HTTP, para que la API nunca devuelva un 500
 * con una traza de Java en el cuerpo (igual que en rabbitadmin).
 */
@RestControllerAdvice
public class ManejadorDeErrores {

    private static final Logger log = LoggerFactory.getLogger(ManejadorDeErrores.class);

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<RespuestaError> noEncontrado(RecursoNoEncontradoException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new RespuestaError(e.getMessage(), null));
    }

    /** Topico protegido, topico que ya existe, grupo con consumidores activos. */
    @ExceptionHandler(OperacionNoPermitidaException.class)
    public ResponseEntity<RespuestaError> noPermitida(OperacionNoPermitidaException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new RespuestaError(e.getMessage(), null));
    }

    /** Un cuerpo que no es JSON en el PUT de configuracion. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<RespuestaError> cuerpoIlegible(HttpMessageNotReadableException e) {
        return ResponseEntity.badRequest().body(new RespuestaError(
                "El cuerpo debe ser un JSON, por ejemplo {\"retention.ms\": \"86400000\"}", "body"));
    }

    /**
     * Lo que rechaza el propio cluster. Una solicitud que el cluster considera
     * invalida -por ejemplo, factor de replicacion mayor que la cantidad de
     * brokers vivos- es un 400; que el cluster no responda es un 503.
     */
    @ExceptionHandler(KafkaOperacionException.class)
    public ResponseEntity<RespuestaError> fallaDeKafka(KafkaOperacionException e) {
        Throwable causa = e.getCause();
        if (causa instanceof InvalidReplicationFactorException || causa instanceof InvalidPartitionsException
                || causa instanceof InvalidConfigurationException || causa instanceof InvalidTopicException
                || causa instanceof InvalidRequestException || causa instanceof PolicyViolationException) {
            return ResponseEntity.badRequest().body(new RespuestaError("Kafka rechazo la solicitud: " + causa.getMessage(), null));
        }
        if (causa instanceof TimeoutException || causa instanceof org.apache.kafka.common.errors.TimeoutException) {
            log.error("El cluster Kafka no respondio", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new RespuestaError("El cluster Kafka no respondio: " + causa.getMessage(), null));
        }
        log.error("Kafka rechazo la operacion", e);
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new RespuestaError("Kafka rechazo la operacion: " + (causa == null ? e.getMessage() : causa.getMessage()), null));
    }
}
