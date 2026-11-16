package cl.duoc.pedidos360.kafkaadmin.controller;

import cl.duoc.pedidos360.kafkaadmin.dto.ErrorResponse;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService.KafkaOperacionException;
import cl.duoc.pedidos360.kafkaadmin.service.OperacionNoPermitidaException;
import cl.duoc.pedidos360.kafkaadmin.service.RecursoNoEncontradoException;
import cl.duoc.pedidos360.kafkaadmin.service.SolicitudInvalidaException;
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
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Comparator;
import java.util.concurrent.TimeoutException;

/**
 * Respuestas de error consistentes (400, 404, 409, 503), la mejora que la guia
 * 3.4.1 propone en su seccion 9: nunca un 500 con una traza de Java, y nunca
 * una respuesta de exito si Kafka rechazo la operacion.
 */
@RestControllerAdvice
public class ManejadorDeErrores {

    private static final Logger log = LoggerFactory.getLogger(ManejadorDeErrores.class);

    /** Un DTO que no pasa @Valid: se nombra el primer campo con error. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> invalido(MethodArgumentNotValidException e) {
        FieldError campo = e.getBindingResult().getFieldErrors().stream()
                .min(Comparator.comparing(FieldError::getField))
                .orElse(null);
        if (campo == null) {
            return ResponseEntity.badRequest().body(new ErrorResponse("La solicitud no es valida", null));
        }
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(campo.getField() + " " + campo.getDefaultMessage(), campo.getField()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> cuerpoIlegible(HttpMessageNotReadableException e) {
        return ResponseEntity.badRequest().body(new ErrorResponse(
                "El cuerpo debe ser un JSON valido, como los ejemplos de la guia 3.4.1", "body"));
    }

    @ExceptionHandler(SolicitudInvalidaException.class)
    public ResponseEntity<ErrorResponse> solicitudInvalida(SolicitudInvalidaException e) {
        return ResponseEntity.badRequest().body(new ErrorResponse(e.getMessage(), e.getParametro()));
    }

    @ExceptionHandler(RecursoNoEncontradoException.class)
    public ResponseEntity<ErrorResponse> noEncontrado(RecursoNoEncontradoException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorResponse(e.getMessage(), null));
    }

    /** Topico protegido, topico que ya existe, grupo con consumidores activos. */
    @ExceptionHandler(OperacionNoPermitidaException.class)
    public ResponseEntity<ErrorResponse> noPermitida(OperacionNoPermitidaException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(e.getMessage(), null));
    }

    /**
     * Lo que rechaza el propio cluster. Una solicitud que el cluster considera
     * invalida -por ejemplo, factor de replicacion mayor que los brokers vivos o
     * una propiedad que no existe- es un 400; que el cluster no responda, un 503.
     */
    @ExceptionHandler(KafkaOperacionException.class)
    public ResponseEntity<ErrorResponse> fallaDeKafka(KafkaOperacionException e) {
        Throwable causa = e.getCause();
        if (causa instanceof InvalidReplicationFactorException || causa instanceof InvalidPartitionsException
                || causa instanceof InvalidConfigurationException || causa instanceof InvalidTopicException
                || causa instanceof InvalidRequestException || causa instanceof PolicyViolationException) {
            return ResponseEntity.badRequest()
                    .body(new ErrorResponse("Kafka rechazo la solicitud: " + causa.getMessage(), null));
        }
        if (causa instanceof TimeoutException || causa instanceof org.apache.kafka.common.errors.TimeoutException) {
            log.error("El cluster Kafka no respondio", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new ErrorResponse("El cluster Kafka no respondio: " + causa.getMessage(), null));
        }
        log.error("Kafka rechazo la operacion", e);
        return ResponseEntity.status(HttpStatus.CONFLICT).body(new ErrorResponse(
                "Kafka rechazo la operacion: " + (causa == null ? e.getMessage() : causa.getMessage()), null));
    }
}
