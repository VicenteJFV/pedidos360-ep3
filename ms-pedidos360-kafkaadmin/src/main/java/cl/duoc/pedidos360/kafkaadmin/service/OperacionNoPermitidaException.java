package cl.duoc.pedidos360.kafkaadmin.service;

/**
 * La operacion es valida pero choca con el estado del cluster o con una
 * proteccion del caso: eliminar un topico funcional, crear uno que ya existe,
 * borrar un grupo con consumidores activos. Se responde 409.
 */
public class OperacionNoPermitidaException extends RuntimeException {

    public OperacionNoPermitidaException(String mensaje) {
        super(mensaje);
    }
}
