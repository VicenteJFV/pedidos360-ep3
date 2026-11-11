package cl.duoc.pedidos360.reportes.consumer;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Valida el evento antes de procesarlo (seccion 7 de la guia), con las
 * anotaciones del DTO y Validator.validate, como el EventoConsumer de la guia
 * practica 3.3.2.
 */
@Component
public class ValidadorEventos {

    private final Validator validator;

    public ValidadorEventos(Validator validator) {
        this.validator = validator;
    }

    /**
     * @return el instante del evento, ya leido
     * @throws EventoInvalidoException con el detalle de cada campo que falla
     */
    public <T> Instant validar(T evento, String occurredAt) {
        if (evento == null) {
            // Un registro sin valor (tombstone) o un JSON "null".
            throw new EventoInvalidoException("El registro no trae contenido");
        }

        Set<ConstraintViolation<T>> errores = validator.validate(evento);
        if (!errores.isEmpty()) {
            String detalle = errores.stream()
                    .map(e -> e.getPropertyPath() + ": " + e.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new EventoInvalidoException("DTO invalido: " + detalle);
        }

        try {
            return Instant.parse(occurredAt);
        } catch (DateTimeParseException e) {
            throw new EventoInvalidoException("DTO invalido: occurredAt no es una fecha ISO-8601: " + occurredAt);
        }
    }
}
