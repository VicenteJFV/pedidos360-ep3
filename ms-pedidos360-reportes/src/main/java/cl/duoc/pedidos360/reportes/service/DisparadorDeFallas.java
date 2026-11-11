package cl.duoc.pedidos360.reportes.service;

import org.springframework.stereotype.Component;

/**
 * Fallo controlado para demostrar reintentos y DLT, como el tipo "FALLA_..."
 * de la guia practica 3.3.2.
 *
 * Aqui el disparador es un dato del flujo real: el correo del cliente. Se crea
 * un pedido con error-kafka@pedidos360.cl y su evento falla en la reporteria
 * en cada intento, agota los reintentos y termina en pedidos-eventos-dlt. Los
 * demas pedidos se siguen procesando.
 *
 * El correo no contiene "error-permanente" ni "error-transitorio", los
 * disparadores de EP2: RabbitMQ procesa ese pedido con normalidad. Asi cada
 * demostracion afecta a un solo sistema de mensajeria.
 */
@Component
public class DisparadorDeFallas {

    static final String MARCA = "error-kafka";

    public void evaluar(String clienteEmail, String eventId) {
        if (clienteEmail != null && clienteEmail.toLowerCase().contains(MARCA)) {
            throw new FalloControladoException(
                    "Fallo controlado al actualizar la reporteria. eventId=" + eventId
                            + ". Puede funcionar en el siguiente intento");
        }
    }

    /** Falla recuperable: el DefaultErrorHandler la reintenta antes de mandarla al DLT. */
    public static class FalloControladoException extends RuntimeException {
        public FalloControladoException(String mensaje) {
            super(mensaje);
        }
    }
}
