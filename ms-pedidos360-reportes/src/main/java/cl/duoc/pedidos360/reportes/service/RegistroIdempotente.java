package cl.duoc.pedidos360.reportes.service;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Idempotencia del consumidor, con la tabla PROCESSED_MESSAGES de la guia
 * practica 3.3.2.
 *
 * Kafka entrega "al menos una vez": tras un reinicio o un rebalanceo, un evento
 * ya procesado puede volver a llegar porque su offset aun no se habia
 * confirmado. Registrar el eventId con clave primaria hace que la segunda
 * llegada se reconozca y se ignore.
 *
 * A diferencia del ejercicio de la guia, el registro va en la MISMA transaccion
 * que la actualizacion del reporte (los servicios son @Transactional). La guia
 * advierte que si el procesamiento falla despues del INSERT, el reintento se
 * confundiria con un duplicado; con una sola transaccion, ese fallo revierte
 * tambien el INSERT y el reintento se procesa de nuevo.
 */
@Component
public class RegistroIdempotente {

    private final JdbcTemplate jdbc;

    public RegistroIdempotente(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @return true si el evento es nuevo; false si ya se habia procesado
     */
    public boolean marcarProcesado(String eventId) {
        try {
            jdbc.update("INSERT INTO PROCESSED_MESSAGES (EVENT_ID) VALUES (?)", eventId);
            return true;
        } catch (DuplicateKeyException e) {
            // Spring traduce a DuplicateKeyException solo la violacion de clave
            // unica (ORA-00001 en Oracle, 23505 en H2). Cualquier otro error de
            // base de datos -conexion, permisos- se propaga: no se esconde como
            // si fuera un duplicado.
            return false;
        }
    }
}
