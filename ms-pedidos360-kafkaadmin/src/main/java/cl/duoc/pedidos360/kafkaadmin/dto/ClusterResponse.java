package cl.duoc.pedidos360.kafkaadmin.dto;

import java.util.List;

/**
 * Los brokers que responden y cual es el controlador del cluster.
 *
 * @param controller id del broker que coordina el cluster (elige los lideres)
 */
public record ClusterResponse(String clusterId, Integer controller, List<Broker> brokers) {

    public record Broker(int id, String host, int port) {
    }
}
