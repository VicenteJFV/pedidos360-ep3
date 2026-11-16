package cl.duoc.pedidos360.kafkaadmin.controller;

import cl.duoc.pedidos360.kafkaadmin.dto.ClusterResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.ConsumerGroupDetailResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.ConsumerGroupResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.CreateTopicRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.IncreasePartitionsRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.MetricsResponse;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicConfigRequest;
import cl.duoc.pedidos360.kafkaadmin.dto.TopicDescriptionResponse;
import cl.duoc.pedidos360.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * API del administrador Kafka. Los endpoints de la guia practica 3.4.1:
 *
 *   GET    /api/kafka/topics                       lista los topicos
 *   GET    /api/kafka/topics/{name}                particiones, lider, replicas e ISR
 *   POST   /api/kafka/topics                       crea un topico (cuerpo CreateTopicRequest)
 *   GET    /api/kafka/topics/{name}/configs        consulta la configuracion
 *   PATCH  /api/kafka/topics/{name}/configs        modifica una configuracion
 *   PATCH  /api/kafka/topics/{name}/partitions     aumenta las particiones
 *   GET    /api/kafka/consumer-groups              lista los grupos
 *   DELETE /api/kafka/topics/{name}                elimina un topico de prueba
 *
 * Y los que agrega el caso para supervisar Kafka (seccion 6.2 de la guia de EP3):
 *
 *   GET    /api/kafka/consumer-groups/{groupId}    miembros, offsets y lag
 *   DELETE /api/kafka/consumer-groups/{groupId}    elimina un grupo de prueba sin miembros
 *   GET    /api/kafka/cluster                      brokers y controlador
 *   GET    /api/kafka/metricas                     vista rapida de topicos y grupos del caso
 *
 * La validacion de entrada usa @Valid sobre los DTO, como la guia; los errores
 * se responden con 400, 404 o 409 desde ManejadorDeErrores.
 */
@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/topics")
    public List<String> topics() {
        return service.listTopics();
    }

    @GetMapping("/topics/{name}")
    public TopicDescriptionResponse describe(@PathVariable String name) {
        return service.describeTopic(name);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> create(@Valid @RequestBody CreateTopicRequest r) {
        service.createTopic(r);
        return Map.of("message", "Topic creado", "topic", r.name());
    }

    @GetMapping("/topics/{name}/configs")
    public Map<String, String> configs(@PathVariable String name) {
        return service.topicConfigs(name);
    }

    @PatchMapping("/topics/{name}/configs")
    public Map<String, String> updateConfig(@PathVariable String name, @Valid @RequestBody TopicConfigRequest r) {
        service.updateConfig(name, r.configName(), r.value());
        return Map.of("message", "Configuracion actualizada", "topic", name,
                "config", r.configName(), "value", r.value());
    }

    @PatchMapping("/topics/{name}/partitions")
    public Map<String, String> partitions(@PathVariable String name, @Valid @RequestBody IncreasePartitionsRequest r) {
        service.increasePartitions(name, r.totalPartitions());
        return Map.of("message", "Particiones aumentadas", "topic", name,
                "totalPartitions", r.totalPartitions().toString());
    }

    @DeleteMapping("/topics/{name}")
    public Map<String, String> delete(@PathVariable String name) {
        service.deleteTopic(name);
        return Map.of("message", "Topic eliminado", "topic", name);
    }

    @GetMapping("/consumer-groups")
    public List<ConsumerGroupResponse> groups() {
        return service.consumerGroups();
    }

    @GetMapping("/consumer-groups/{groupId}")
    public ConsumerGroupDetailResponse group(@PathVariable String groupId) {
        return service.consumerGroup(groupId);
    }

    @DeleteMapping("/consumer-groups/{groupId}")
    public Map<String, String> deleteGroup(@PathVariable String groupId) {
        service.deleteConsumerGroup(groupId);
        return Map.of("message", "Consumer group eliminado", "groupId", groupId);
    }

    @GetMapping("/cluster")
    public ClusterResponse cluster() {
        return service.cluster();
    }

    @GetMapping("/metricas")
    public MetricsResponse metrics() {
        return service.metrics();
    }
}
