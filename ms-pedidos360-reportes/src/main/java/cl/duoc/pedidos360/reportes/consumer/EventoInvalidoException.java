package cl.duoc.pedidos360.reportes.consumer;

/**
 * El evento no cumple el contrato: falta un campo, un valor no es valido o la
 * fecha no se puede leer.
 *
 * KafkaErrorConfig la marca como NO reintentable: el mismo mensaje va a fallar
 * igual en cada intento, asi que reintentar solo atrasa a los que vienen
 * detras. Va directo al DLT.
 */
public class EventoInvalidoException extends RuntimeException {

    public EventoInvalidoException(String mensaje) {
        super(mensaje);
    }
}
