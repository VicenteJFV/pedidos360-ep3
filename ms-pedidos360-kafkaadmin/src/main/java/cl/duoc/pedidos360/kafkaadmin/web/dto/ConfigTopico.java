package cl.duoc.pedidos360.kafkaadmin.web.dto;

import java.util.Map;

/**
 * Configuraciones administrables de un topico.
 *
 * @param configuraciones nombre de la propiedad -> valor y de donde sale
 */
public record ConfigTopico(String topico, Map<String, Valor> configuraciones) {

    /**
     * @param origen DYNAMIC_TOPIC_CONFIG si se fijo para este topico;
     *               DEFAULT_CONFIG o STATIC_BROKER_CONFIG si lo hereda del broker
     */
    public record Valor(String valor, String origen) {
    }
}
