-- Tablas de ms-pedidos360-reportes. Tipos comunes a Oracle y H2: el mismo
-- script sirve para el perfil oracle y para el perfil dev.

-- Idempotencia: la tabla de la fase final de la guia practica 3.3.2. Un eventId
-- se registra una sola vez; si el mismo evento vuelve a llegar -Kafka entrega
-- "al menos una vez"-, el INSERT choca con la clave primaria y se ignora.
CREATE TABLE PROCESSED_MESSAGES (
    EVENT_ID VARCHAR(100) PRIMARY KEY,
    PROCESSED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Reporteria: el ultimo estado conocido de cada pedido, alimentado por
-- pedidos-eventos.
CREATE TABLE REPORTE_PEDIDOS (
    CODIGO            VARCHAR(40) PRIMARY KEY,
    PEDIDO_ID         NUMERIC(19),
    ESTADO            VARCHAR(20) NOT NULL,
    CLIENTE_EMAIL     VARCHAR(150),
    TOTAL             NUMERIC(14, 2),
    CANTIDAD_ITEMS    NUMERIC(5),
    ULTIMO_EVENTO_EN  TIMESTAMP NOT NULL,
    EVENTOS_RECIBIDOS NUMERIC(5) NOT NULL
);

-- Auditoria: una fila por accion, alimentada por pedidos-auditoria. Guarda
-- tambien en que particion y offset llego, para cruzarlo con Kafka UI.
CREATE TABLE AUDITORIA_PEDIDOS (
    EVENT_ID          VARCHAR(100) PRIMARY KEY,
    CODIGO_PEDIDO     VARCHAR(40) NOT NULL,
    ACCION            VARCHAR(20) NOT NULL,
    ESTADO_ANTERIOR   VARCHAR(20),
    ESTADO_NUEVO      VARCHAR(20) NOT NULL,
    USUARIO           VARCHAR(150) NOT NULL,
    ORIGEN            VARCHAR(60) NOT NULL,
    DETALLE           VARCHAR(500),
    EVENTO_NEGOCIO_ID VARCHAR(100),
    OCURRIDO_EN       TIMESTAMP NOT NULL,
    PARTICION         NUMERIC(5),
    OFFSET_KAFKA      NUMERIC(19)
);

CREATE INDEX IDX_AUDITORIA_CODIGO ON AUDITORIA_PEDIDOS (CODIGO_PEDIDO);
