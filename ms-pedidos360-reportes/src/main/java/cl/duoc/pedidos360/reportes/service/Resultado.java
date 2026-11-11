package cl.duoc.pedidos360.reportes.service;

/** Lo que paso con un evento recibido. */
public enum Resultado {

    /** Se proceso y actualizo la informacion. */
    PROCESADO,

    /** Ya se habia procesado antes: se ignora sin efecto. */
    DUPLICADO,

    /**
     * Es valido, pero mas antiguo que el ultimo estado conocido del pedido
     * -por ejemplo, un evento reprocesado desde el DLT-: se registra sin
     * retroceder el estado del reporte.
     */
    ANTERIOR_AL_ESTADO_ACTUAL
}
