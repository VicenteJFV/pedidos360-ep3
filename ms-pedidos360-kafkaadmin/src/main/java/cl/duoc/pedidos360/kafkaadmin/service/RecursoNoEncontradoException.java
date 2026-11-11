package cl.duoc.pedidos360.kafkaadmin.service;

/** El topico o el consumer group no existe. Se responde 404. */
public class RecursoNoEncontradoException extends RuntimeException {

    public RecursoNoEncontradoException(String mensaje) {
        super(mensaje);
    }
}
