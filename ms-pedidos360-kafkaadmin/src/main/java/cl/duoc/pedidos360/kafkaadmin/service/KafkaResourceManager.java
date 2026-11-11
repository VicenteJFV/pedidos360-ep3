package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.config.TopicosDelCaso;
import cl.duoc.pedidos360.kafkaadmin.web.dto.ConfigTopico;
import cl.duoc.pedidos360.kafkaadmin.web.dto.InfoCluster;
import cl.duoc.pedidos360.kafkaadmin.web.dto.InfoGrupo;
import cl.duoc.pedidos360.kafkaadmin.web.dto.InfoTopico;
import cl.duoc.pedidos360.kafkaadmin.web.dto.MetricasKafka;
import cl.duoc.pedidos360.kafkaadmin.web.dto.ResumenGrupo;
import cl.duoc.pedidos360.kafkaadmin.web.dto.ResumenTopico;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Administracion de Kafka sobre el AdminClient. Es el equivalente de
 * RabbitResourceManager: recibe parametros ya validados por el controlador y
 * traduce las respuestas del cluster a los DTO de la API.
 *
 * Cada operacion del AdminClient es asincrona y devuelve un KafkaFuture; aqui
 * se espera el resultado con un limite de tiempo, para que una caida del
 * cluster se convierta en un error claro y no en una peticion colgada.
 */
@Service
public class KafkaResourceManager {

    private static final Logger log = LoggerFactory.getLogger(KafkaResourceManager.class);

    /**
     * Las configuraciones que la API muestra y deja modificar (seccion 6.1 de la
     * guia). Las demas existen, pero no son parte de la administracion del caso.
     */
    public static final List<String> CONFIGURACIONES_ADMINISTRABLES = List.of(
            "retention.ms", "cleanup.policy", "min.insync.replicas", "retention.bytes", "max.message.bytes");

    private final AdminClient admin;
    private final long timeoutSegundos;

    public KafkaResourceManager(AdminClient admin, @Value("${kafkaadmin.timeout-segundos:10}") long timeoutSegundos) {
        this.admin = admin;
        this.timeoutSegundos = timeoutSegundos;
    }

    // -----------------------------------------------------------------------
    // Cluster
    // -----------------------------------------------------------------------

    public InfoCluster cluster() {
        DescribeClusterResult descripcion = admin.describeCluster();
        Collection<Node> nodos = esperar(descripcion.nodes());
        Node controlador = esperar(descripcion.controller());
        List<InfoCluster.Broker> brokers = nodos.stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(n -> new InfoCluster.Broker(n.id(), n.host(), n.port()))
                .toList();
        return new InfoCluster(esperar(descripcion.clusterId()),
                controlador == null ? null : controlador.id(), brokers);
    }

    // -----------------------------------------------------------------------
    // Topicos
    // -----------------------------------------------------------------------

    public List<ResumenTopico> listarTopicos(boolean incluirInternos) {
        Set<String> nombres = esperar(admin.listTopics(new ListTopicsOptions().listInternal(incluirInternos)).names());
        if (nombres.isEmpty()) {
            return List.of();
        }
        return esperar(admin.describeTopics(nombres).allTopicNames()).values().stream()
                .filter(t -> incluirInternos || !TopicosDelCaso.esInterno(t.name()))
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(t -> new ResumenTopico(t.name(), t.partitions().size(), factorDeReplicacion(t)))
                .toList();
    }

    public InfoTopico consultarTopico(String nombre) {
        TopicDescription descripcion = describir(nombre);
        List<TopicPartition> particiones = descripcion.partitions().stream()
                .map(p -> new TopicPartition(nombre, p.partition()))
                .toList();
        Map<TopicPartition, Long> iniciales = offsets(particiones, OffsetSpec.earliest());
        Map<TopicPartition, Long> finales = offsets(particiones, OffsetSpec.latest());

        List<InfoTopico.InfoParticion> detalle = new ArrayList<>();
        long mensajes = 0;
        int sinIsr = 0;
        for (TopicPartitionInfo p : descripcion.partitions()) {
            TopicPartition tp = new TopicPartition(nombre, p.partition());
            long inicial = iniciales.getOrDefault(tp, 0L);
            long fin = finales.getOrDefault(tp, 0L);
            mensajes += fin - inicial;
            if (p.isr().size() < p.replicas().size()) {
                sinIsr++;
            }
            detalle.add(new InfoTopico.InfoParticion(
                    p.partition(),
                    p.leader() == null || p.leader().isEmpty() ? null : p.leader().id(),
                    ids(p.replicas()),
                    ids(p.isr()),
                    inicial,
                    fin));
        }
        return new InfoTopico(nombre, descripcion.partitions().size(), factorDeReplicacion(descripcion),
                mensajes, sinIsr, detalle);
    }

    public void crearTopico(String nombre, int particiones, short factorReplicacion, Map<String, String> configuraciones) {
        NewTopic nuevo = new NewTopic(nombre, particiones, factorReplicacion).configs(configuraciones);
        try {
            esperar(admin.createTopics(List.of(nuevo)).all());
        } catch (KafkaOperacionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                throw new OperacionNoPermitidaException("El topico " + nombre + " ya existe");
            }
            throw e;
        }
        log.info("Topico creado | nombre: {} | particiones: {} | factor de replicacion: {} | configuraciones: {}",
                nombre, particiones, factorReplicacion, configuraciones);
    }

    /**
     * Elimina un topico de prueba. Los topicos del caso y sus DLT estan
     * protegidos: la guia pide demostrar la eliminacion sin afectarlos.
     */
    public void eliminarTopico(String nombre) {
        if (TopicosDelCaso.PROTEGIDOS.contains(nombre) || TopicosDelCaso.esInterno(nombre)) {
            throw new OperacionNoPermitidaException("El topico " + nombre
                    + " es parte del caso y no se elimina desde la API. Para probar la eliminacion, usa un topico de prueba");
        }
        describir(nombre); // 404 si no existe
        esperar(admin.deleteTopics(List.of(nombre)).all());
        log.info("Topico eliminado | nombre: {}", nombre);
    }

    // -----------------------------------------------------------------------
    // Configuraciones
    // -----------------------------------------------------------------------

    public ConfigTopico consultarConfiguracion(String nombre) {
        describir(nombre);
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = esperar(admin.describeConfigs(List.of(recurso)).all()).get(recurso);

        Map<String, ConfigTopico.Valor> valores = new LinkedHashMap<>();
        for (String clave : CONFIGURACIONES_ADMINISTRABLES) {
            ConfigEntry entrada = config.get(clave);
            if (entrada != null) {
                valores.put(clave, new ConfigTopico.Valor(entrada.value(), entrada.source().name()));
            }
        }
        return new ConfigTopico(nombre, valores);
    }

    /**
     * Cambia solo las propiedades indicadas (incrementalAlterConfigs con SET):
     * las demas conservan su valor. Devuelve la configuracion leida de nuevo
     * desde el cluster, para comprobar el resultado (seccion 6 de la guia).
     */
    public ConfigTopico modificarConfiguracion(String nombre, Map<String, String> cambios) {
        describir(nombre);
        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        List<AlterConfigOp> operaciones = cambios.entrySet().stream()
                .map(c -> new AlterConfigOp(new ConfigEntry(c.getKey(), c.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        esperar(admin.incrementalAlterConfigs(Map.of(recurso, operaciones)).all());
        log.info("Configuracion modificada | topico: {} | cambios: {}", nombre, cambios);
        return consultarConfiguracion(nombre);
    }

    // -----------------------------------------------------------------------
    // Consumer groups
    // -----------------------------------------------------------------------

    @SuppressWarnings("deprecation")
    public List<ResumenGrupo> listarGrupos() {
        List<String> ids = esperar(admin.listConsumerGroups().all()).stream()
                .map(g -> g.groupId())
                .sorted()
                .toList();
        if (ids.isEmpty()) {
            return List.of();
        }
        return esperar(admin.describeConsumerGroups(ids).all()).values().stream()
                .sorted(Comparator.comparing(ConsumerGroupDescription::groupId))
                .map(g -> new ResumenGrupo(g.groupId(), g.groupState().name(), g.members().size()))
                .toList();
    }

    public InfoGrupo consultarGrupo(String grupo) {
        ConsumerGroupDescription descripcion = describirGrupo(grupo);

        List<InfoGrupo.Miembro> miembros = descripcion.members().stream()
                .map(m -> new InfoGrupo.Miembro(m.consumerId(), m.clientId(), m.host(),
                        m.assignment().topicPartitions().stream()
                                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                                .map(TopicPartition::toString)
                                .toList()))
                .sorted(Comparator.comparing(InfoGrupo.Miembro::clientId))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> confirmados =
                esperar(admin.listConsumerGroupOffsets(grupo).partitionsToOffsetAndMetadata());
        Map<TopicPartition, Long> finales = offsets(confirmados.keySet(), OffsetSpec.latest());

        List<InfoGrupo.OffsetParticion> avance = confirmados.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .sorted(Map.Entry.comparingByKey(
                        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition)))
                .map(e -> {
                    long consumido = e.getValue().offset();
                    long fin = finales.getOrDefault(e.getKey(), consumido);
                    return new InfoGrupo.OffsetParticion(e.getKey().topic(), e.getKey().partition(),
                            consumido, fin, Math.max(0, fin - consumido));
                })
                .toList();
        long lagTotal = avance.stream().mapToLong(InfoGrupo.OffsetParticion::lag).sum();

        return new InfoGrupo(grupo, descripcion.groupState().name(),
                descripcion.coordinator() == null ? null : descripcion.coordinator().id(),
                lagTotal, miembros, avance);
    }

    /**
     * Elimina un consumer group de prueba. Kafka solo lo permite si el grupo no
     * tiene miembros activos; los grupos de ms-pedidos360-reportes estan
     * protegidos, porque perder sus offsets haria reprocesar todo el topico.
     */
    public void eliminarGrupo(String grupo) {
        if (TopicosDelCaso.GRUPOS_PROTEGIDOS.contains(grupo)) {
            throw new OperacionNoPermitidaException("El consumer group " + grupo
                    + " es de ms-pedidos360-reportes y no se elimina desde la API");
        }
        describirGrupo(grupo);
        try {
            esperar(admin.deleteConsumerGroups(List.of(grupo)).all());
        } catch (KafkaOperacionException e) {
            if (e.getCause() instanceof GroupNotEmptyException) {
                throw new OperacionNoPermitidaException("El consumer group " + grupo
                        + " tiene consumidores activos; detenlos antes de eliminarlo");
            }
            throw e;
        }
        log.info("Consumer group eliminado | grupo: {}", grupo);
    }

    // -----------------------------------------------------------------------
    // Metricas
    // -----------------------------------------------------------------------

    public MetricasKafka metricas() {
        int brokers = esperar(admin.describeCluster().nodes()).size();

        List<MetricasKafka.Topico> topicos = new ArrayList<>();
        for (String nombre : List.of(TopicosDelCaso.EVENTOS, TopicosDelCaso.AUDITORIA,
                TopicosDelCaso.EVENTOS_DLT, TopicosDelCaso.AUDITORIA_DLT)) {
            try {
                InfoTopico info = consultarTopico(nombre);
                topicos.add(new MetricasKafka.Topico(nombre, info.mensajes(), info.particionesSinIsr()));
            } catch (RecursoNoEncontradoException e) {
                // Un topico del caso que no existe no rompe las metricas del resto
            }
        }

        List<MetricasKafka.Grupo> grupos = new ArrayList<>();
        for (String grupo : List.of(TopicosDelCaso.GRUPO_REPORTES, TopicosDelCaso.GRUPO_AUDITORIA)) {
            try {
                InfoGrupo info = consultarGrupo(grupo);
                grupos.add(new MetricasKafka.Grupo(grupo, info.estado(), info.miembros().size(), info.lagTotal()));
            } catch (RecursoNoEncontradoException e) {
                // El grupo aparece recien cuando ms-pedidos360-reportes se conecta
            }
        }
        return new MetricasKafka(brokers, topicos, grupos);
    }

    // -----------------------------------------------------------------------

    private TopicDescription describir(String nombre) {
        try {
            return esperar(admin.describeTopics(List.of(nombre)).allTopicNames()).get(nombre);
        } catch (KafkaOperacionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new RecursoNoEncontradoException("No existe el topico " + nombre);
            }
            throw e;
        }
    }

    private ConsumerGroupDescription describirGrupo(String grupo) {
        ConsumerGroupDescription descripcion;
        try {
            descripcion = esperar(admin.describeConsumerGroups(List.of(grupo)).all()).get(grupo);
        } catch (KafkaOperacionException e) {
            if (e.getCause() instanceof GroupIdNotFoundException) {
                throw new RecursoNoEncontradoException("No existe el consumer group " + grupo);
            }
            throw e;
        }
        // Kafka responde DEAD -en vez de error- para un grupo que nunca existio
        if (descripcion == null || "DEAD".equals(descripcion.groupState().name())) {
            throw new RecursoNoEncontradoException("No existe el consumer group " + grupo);
        }
        return descripcion;
    }

    private Map<TopicPartition, Long> offsets(Collection<TopicPartition> particiones, OffsetSpec spec) {
        if (particiones.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> consulta = particiones.stream()
                .collect(Collectors.toMap(Function.identity(), p -> spec));
        Map<TopicPartition, ListOffsetsResultInfo> resultado = esperar(admin.listOffsets(consulta).all());
        return resultado.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    private static int factorDeReplicacion(TopicDescription topico) {
        return topico.partitions().isEmpty() ? 0 : topico.partitions().get(0).replicas().size();
    }

    private static List<Integer> ids(List<Node> nodos) {
        return nodos.stream().map(Node::id).sorted().toList();
    }

    private <T> T esperar(KafkaFuture<T> futuro) {
        try {
            return futuro.get(timeoutSegundos, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new KafkaOperacionException(e.getCause());
        } catch (TimeoutException e) {
            throw new KafkaOperacionException(new TimeoutException(
                    "El cluster Kafka no respondio en " + timeoutSegundos + " s"));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaOperacionException(e);
        }
    }

    /**
     * Un error que devolvio el cluster. La causa es la excepcion original de
     * Kafka (InvalidReplicationFactorException, TimeoutException...), que el
     * ManejadorDeErrores traduce a un codigo HTTP.
     */
    public static class KafkaOperacionException extends RuntimeException {
        public KafkaOperacionException(Throwable causa) {
            super(causa == null ? "Error de Kafka" : causa.getMessage(), causa);
        }
    }
}
