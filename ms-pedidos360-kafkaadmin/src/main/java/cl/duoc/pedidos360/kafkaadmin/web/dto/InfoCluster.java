package cl.duoc.pedidos360.kafkaadmin.web.dto;

import java.util.List;

/**
 * Los brokers que responden y cual es el controlador del cluster.
 *
 * @param controlador id del broker que coordina el cluster (asigna lideres)
 */
public record InfoCluster(String clusterId, Integer controlador, List<Broker> brokers) {

    public record Broker(int id, String host, int puerto) {
    }
}
