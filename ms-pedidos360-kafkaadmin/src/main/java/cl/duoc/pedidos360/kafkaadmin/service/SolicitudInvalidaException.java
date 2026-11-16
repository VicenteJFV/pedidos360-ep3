package cl.duoc.pedidos360.kafkaadmin.service;

/**
 * La solicitud no tiene sentido para el estado del topico o del cluster: un
 * valor de configuracion fuera de rango, reducir particiones. Se responde 400
 * nombrando el campo.
 */
public class SolicitudInvalidaException extends RuntimeException {

    private final String parametro;

    public SolicitudInvalidaException(String mensaje, String parametro) {
        super(mensaje);
        this.parametro = parametro;
    }

    public String getParametro() {
        return parametro;
    }
}
