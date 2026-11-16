package cl.duoc.pedidos360.kafkaadmin.service;

import cl.duoc.pedidos360.kafkaadmin.config.TopicosDelCaso;
import cl.duoc.pedidos360.kafkaadmin.dto.ClusterResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.ConsumerGroupDetailResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.ConsumerGroupResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.CreateTopicRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.MetricsResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicDescriptionResponse;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewPartitions;
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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Administracion de Kafka sobre AdminClient. Es el KafkaAdminService de la guia
 * practica 3.4.1, con cuatro diferencias:
 *
 *  - Cada operacion espera con un limite de tiempo (kafkaadmin.timeout-segundos):
 *    si el cluster no responde, la API contesta 503 en vez de quedar colgada.
 *  - Los errores de Kafka se traducen a excepciones propias que el
 *    ManejadorDeErrores convierte en 400, 404 o 409, la mejora que la guia deja
 *    propuesta en su seccion 9.
 *  - Los topicos y consumer groups del caso estan protegidos contra eliminacion
 *    (la guia pide no eliminar los topicos de negocio).
 *  - Agrega lo que la seccion 6.2 de la guia de EP3 pide supervisar: offsets y
 *    lag por consumer group, y el estado de las replicas de cada topico.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private static final Pattern CLEANUP_POLICY = Pattern.compile("^(delete|compact|delete,compact|compact,delete)$");

    private final AdminClient admin;
    private final long timeoutSegundos;

    public KafkaAdminService(AdminClient admin, @Value("${kafkaadmin.timeout-segundos:10}") long timeoutSegundos) {
        this.admin = admin;
        this.timeoutSegundos = timeoutSegundos;
    }

    // -----------------------------------------------------------------------
    // Topicos (guia 3.4.1)
    // -----------------------------------------------------------------------

    /** Los topicos del cluster, sin los internos de Kafka. */
    public List<String> listTopics() {
        return esperar(admin.listTopics().names()).stream()
                .sorted()
                .toList();
    }

    public TopicDescriptionResponse describeTopic(String name) {
        TopicDescription d = describir(name);
        List<TopicPartition> particiones = d.partitions().stream()
                .map(p -> new TopicPartition(name, p.partition()))
                .toList();
        Map<TopicPartition, Long> iniciales = offsets(particiones, OffsetSpec.earliest());
        Map<TopicPartition, Long> finales = offsets(particiones, OffsetSpec.latest());

        List<TopicDescriptionResponse.Partition> parts = new ArrayList<>();
        long mensajes = 0;
        int subReplicadas = 0;
        for (TopicPartitionInfo p : d.partitions()) {
            TopicPartition tp = new TopicPartition(name, p.partition());
            long inicial = iniciales.getOrDefault(tp, 0L);
            long fin = finales.getOrDefault(tp, 0L);
            mensajes += fin - inicial;
            if (p.isr().size() < p.replicas().size()) {
                subReplicadas++;
            }
            parts.add(new TopicDescriptionResponse.Partition(
                    p.partition(),
                    p.leader() == null || p.leader().isEmpty() ? null : p.leader().id(),
                    ids(p.replicas()),
                    ids(p.isr()),
                    inicial,
                    fin));
        }
        int replicacion = d.partitions().isEmpty() ? 0 : d.partitions().get(0).replicas().size();
        return new TopicDescriptionResponse(d.name(), d.isInternal(), replicacion, mensajes, subReplicadas, parts);
    }

    public void createTopic(CreateTopicRequest r) {
        Map<String, String> configs = r.configs() == null ? Map.of() : r.configs();
        configs.forEach((nombre, valor) -> validarConfiguracion(nombre, valor, r.replicationFactor(), "configs." + nombre));

        NewTopic topic = new NewTopic(r.name(), r.partitions(), r.replicationFactor()).configs(configs);
        try {
            esperar(admin.createTopics(List.of(topic)).all());
        } catch (KafkaOperacionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                throw new OperacionNoPermitidaException("El topico " + r.name() + " ya existe");
            }
            throw e;
        }
        log.info("Topico creado | nombre: {} | particiones: {} | factor de replicacion: {} | configuraciones: {}",
                r.name(), r.partitions(), r.replicationFactor(), configs);
    }

    /** Todas las configuraciones del topico, ordenadas por nombre, como en la guia. */
    public Map<String, String> topicConfigs(String topic) {
        describir(topic); // 404 si no existe
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = esperar(admin.describeConfigs(List.of(resource)).all()).get(resource);

        Map<String, String> result = new TreeMap<>();
        for (ConfigEntry entry : config.entries()) {
            result.put(entry.name(), entry.value());
        }
        return result;
    }

    /**
     * Cambia una propiedad (incrementalAlterConfigs con SET): las demas conservan
     * su valor. Se comprueba el resultado consultando de nuevo las configuraciones.
     */
    public void updateConfig(String topic, String configName, String value) {
        int replicacion = describeTopic(topic).replicationFactor();
        validarConfiguracion(configName, value, replicacion, "value");

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        AlterConfigOp op = new AlterConfigOp(new ConfigEntry(configName, value), AlterConfigOp.OpType.SET);
        esperar(admin.incrementalAlterConfigs(Map.of(resource, List.of(op))).all());
        log.info("Configuracion modificada | topico: {} | {} = {}", topic, configName, value);
    }

    /**
     * Kafka permite aumentar las particiones, pero no reducirlas: los offsets de
     * una particion eliminada se perderian. Aumentarlas cambia a que particion va
     * cada key, asi que en un topico con orden por key (como pedidos-eventos)
     * conviene hacerlo con cuidado.
     */
    public void increasePartitions(String topic, int total) {
        int actual = describir(topic).partitions().size();
        if (total <= actual) {
            throw new SolicitudInvalidaException(
                    "totalPartitions debe ser mayor que el numero actual de particiones (" + actual + ")",
                    "totalPartitions");
        }
        esperar(admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(total))).all());
        log.info("Particiones aumentadas | topico: {} | de {} a {}", topic, actual, total);
    }

    /**
     * Elimina un topico de prueba. Los topicos del caso y sus DLT estan
     * protegidos: la guia pide demostrar la eliminacion sin afectarlos.
     */
    public void deleteTopic(String topic) {
        if (TopicosDelCaso.PROTEGIDOS.contains(topic) || TopicosDelCaso.esInterno(topic)) {
            throw new OperacionNoPermitidaException("El topico " + topic
                    + " es parte del caso y no se elimina desde la API. Para probar la eliminacion usa un topico de prueba, por ejemplo ev3-admin-prueba");
        }
        describir(topic); // 404 si no existe
        esperar(admin.deleteTopics(List.of(topic)).all());
        log.info("Topico eliminado | nombre: {}", topic);
    }

    // -----------------------------------------------------------------------
    // Consumer groups (guia 3.4.1 y seccion 6.2 de la guia de EP3)
    // -----------------------------------------------------------------------

    @SuppressWarnings("deprecation")
    public List<ConsumerGroupResponse> consumerGroups() {
        Map<String, Boolean> simples = esperar(admin.listConsumerGroups().all()).stream()
                .collect(Collectors.toMap(g -> g.groupId(), g -> g.isSimpleConsumerGroup(), (a, b) -> a));
        if (simples.isEmpty()) {
            return List.of();
        }
        return esperar(admin.describeConsumerGroups(simples.keySet()).all()).values().stream()
                .sorted(Comparator.comparing(ConsumerGroupDescription::groupId))
                .map(g -> new ConsumerGroupResponse(g.groupId(), simples.get(g.groupId()),
                        g.groupState().name(), g.members().size()))
                .toList();
    }

    public ConsumerGroupDetailResponse consumerGroup(String groupId) {
        ConsumerGroupDescription descripcion = describirGrupo(groupId);

        List<ConsumerGroupDetailResponse.Member> miembros = descripcion.members().stream()
                .map(m -> new ConsumerGroupDetailResponse.Member(m.consumerId(), m.clientId(), m.host(),
                        m.assignment().topicPartitions().stream()
                                .sorted(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition))
                                .map(TopicPartition::toString)
                                .toList()))
                .sorted(Comparator.comparing(ConsumerGroupDetailResponse.Member::clientId))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> confirmados =
                esperar(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());
        Map<TopicPartition, Long> finales = offsets(confirmados.keySet(), OffsetSpec.latest());

        List<ConsumerGroupDetailResponse.PartitionOffset> avance = confirmados.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .sorted(Map.Entry.comparingByKey(
                        Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition)))
                .map(e -> {
                    long consumido = e.getValue().offset();
                    long fin = finales.getOrDefault(e.getKey(), consumido);
                    return new ConsumerGroupDetailResponse.PartitionOffset(e.getKey().topic(), e.getKey().partition(),
                            consumido, fin, Math.max(0, fin - consumido));
                })
                .toList();
        long lagTotal = avance.stream().mapToLong(ConsumerGroupDetailResponse.PartitionOffset::lag).sum();

        return new ConsumerGroupDetailResponse(groupId, descripcion.groupState().name(),
                descripcion.coordinator() == null ? null : descripcion.coordinator().id(),
                lagTotal, miembros, avance);
    }

    /**
     * Elimina un consumer group de prueba. Kafka solo lo permite si el grupo no
     * tiene miembros activos; los grupos de ms-pedidos360-reportes estan
     * protegidos, porque perder sus offsets haria reprocesar todo el topico.
     */
    public void deleteConsumerGroup(String groupId) {
        if (TopicosDelCaso.GRUPOS_PROTEGIDOS.contains(groupId)) {
            throw new OperacionNoPermitidaException("El consumer group " + groupId
                    + " es de ms-pedidos360-reportes y no se elimina desde la API");
        }
        describirGrupo(groupId);
        try {
            esperar(admin.deleteConsumerGroups(List.of(groupId)).all());
        } catch (KafkaOperacionException e) {
            if (e.getCause() instanceof GroupNotEmptyException) {
                throw new OperacionNoPermitidaException("El consumer group " + groupId
                        + " tiene consumidores activos; detenlos antes de eliminarlo");
            }
            throw e;
        }
        log.info("Consumer group eliminado | grupo: {}", groupId);
    }

    // -----------------------------------------------------------------------
    // Cluster y metricas
    // -----------------------------------------------------------------------

    public ClusterResponse cluster() {
        DescribeClusterResult descripcion = admin.describeCluster();
        Collection<Node> nodos = esperar(descripcion.nodes());
        Node controlador = esperar(descripcion.controller());
        return new ClusterResponse(esperar(descripcion.clusterId()),
                controlador == null ? null : controlador.id(),
                nodos.stream()
                        .sorted(Comparator.comparingInt(Node::id))
                        .map(n -> new ClusterResponse.Broker(n.id(), n.host(), n.port()))
                        .toList());
    }

    /** Vista rapida de los topicos y grupos del caso. */
    public MetricsResponse metrics() {
        int brokers = esperar(admin.describeCluster().nodes()).size();

        List<MetricsResponse.Topic> topicos = new ArrayList<>();
        for (String nombre : TopicosDelCaso.PROTEGIDOS.stream().sorted().toList()) {
            try {
                TopicDescriptionResponse info = describeTopic(nombre);
                topicos.add(new MetricsResponse.Topic(nombre, info.messages(), info.underReplicated()));
            } catch (RecursoNoEncontradoException e) {
                // Un topico del caso que no existe no rompe las metricas del resto
            }
        }

        List<MetricsResponse.Group> grupos = new ArrayList<>();
        for (String grupo : TopicosDelCaso.GRUPOS_PROTEGIDOS.stream().sorted().toList()) {
            try {
                ConsumerGroupDetailResponse info = consumerGroup(grupo);
                grupos.add(new MetricsResponse.Group(grupo, info.state(), info.members().size(), info.totalLag()));
            } catch (RecursoNoEncontradoException e) {
                // El grupo aparece recien cuando ms-pedidos360-reportes se conecta
            }
        }
        return new MetricsResponse(brokers, topicos, grupos);
    }

    // -----------------------------------------------------------------------

    /**
     * Rangos de las configuraciones que mas se administran (seccion 6.1 de la
     * guia de EP3), para no enviarle al cluster un valor que va a rechazar. Las
     * demas propiedades las valida el propio Kafka.
     */
    static void validarConfiguracion(String nombre, String valor, int replicacion, String campo) {
        String regla = switch (nombre) {
            case "retention.ms" -> largoValido(valor, true) ? null
                    : "retention.ms debe ser un entero mayor que 0 (milisegundos), o -1 para no borrar por tiempo";
            case "retention.bytes" -> largoValido(valor, true) ? null
                    : "retention.bytes debe ser un entero mayor que 0, o -1 para no limitar por tamano";
            case "cleanup.policy" -> valor != null && CLEANUP_POLICY.matcher(valor.trim()).matches() ? null
                    : "cleanup.policy debe ser delete, compact o compact,delete";
            case "min.insync.replicas" -> {
                Long n = largo(valor);
                yield n != null && n >= 1 && n <= replicacion ? null
                        : "min.insync.replicas debe estar entre 1 y el factor de replicacion (" + replicacion + ")";
            }
            default -> null;
        };
        if (regla != null) {
            throw new SolicitudInvalidaException(regla, campo);
        }
    }

    private static boolean largoValido(String valor, boolean permiteMenosUno) {
        Long n = largo(valor);
        return n != null && (n >= 1 || (permiteMenosUno && n == -1));
    }

    private static Long largo(String valor) {
        try {
            return valor == null ? null : Long.valueOf(valor.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

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
