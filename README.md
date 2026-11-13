# ms-analytics-recommendations — PEDIDOS360 EV3

Aplicación ejecutable Java 21 / Spring Boot 3.5.16, PostgreSQL/Flyway y consumidores
Kafka. Rama `ev3/integracion-integral`. Sin despliegue AWS ni imágenes publicadas.

- `docs/ARQUITECTURA.md`: propuesta de responsabilidad, API/eventos y seguridad.
- `src/README.md`: estructura futura.
- `tests/README.md`: criterios, no evidencia de pruebas ejecutadas.
- `deploy/README.md`: propuesta Docker/CI/AWS.

## Ejecución

`mvn -B verify` exige Docker local: tests PostgreSQL y Kafka reales (Testcontainers),
sin saltarlos si Docker falta. Windows Docker Desktop: exportar
`DOCKER_HOST=npipe:////./pipe/dockerDesktopLinuxEngine`. Docker API de test1.44.
`docker build -t analytics:local .` construye localmente, no publica.

Entorno conjunto reproducible: `fazbear-pago/deploy/ev3-local/` en la misma rama.
Ejecutar `compose.yml` propio para ensayo aislado o usar el conjunto de Pago.
Variables: `ANALYTICS_DB_URL/USER/PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`.
API interna :8080. Secretos de BD solo por entorno; no usar datos antiguos/H2.

JWT normal Azure RS256 issuer/tenant/audience/exp/oid UUID, scope access_as_user.
Perfil `ev3-local` (o `local` en tests) permite `LOCAL_JWT_JWK_SET_URI` explícito;
el firmante sintético es solo laboratorio, no autentica usuarios reales de Entra.
Sin roles administrativos o endpoint público para ingerir interacciones.

## REST

- `GET /health`: liveness público.
- `GET /api/recomendaciones/me?limit=6`: identidad del JWT, límite1..20,
  `{estrategia,items:[{productoId,nombre,categoria,precio,score,razon}],generadoEn}`.
- `GET /api/analytics/me/resumen`: conteos propios por tipo y productos distintos;
  no permite elegir otro usuario/tenant ni devuelve identidades ajenas.
- 401 token inválido,403 sin scope,400 límites inválidos. Errores sin SQL/payload.

## Kafka y recomendaciones reales

Consume `producto.actualizado.v1`, `producto.interaccion.v1`, `carrito.actualizado.v1`.
Envelope versionado: eventId UUID,eventType,schemaVersion1,occurredAt UTC,producer,
aggregateId,aggregateVersion,correlationId,data. Catálogo productor ms-productos,
data productoId,nombre,categoria,precio,stock,activo. Interacciones ms-productos o
ms-carrito con tenantId/ownerId pseudónimos,productoId,tipo VIEW/SELECT/ADD/UPDATE/REMOVE.
Se validan productor/tipo/versión/valores; no JWT,email,IP o tarjetas en eventos.

Inbox y proyección commit en una transacción; offset después de commit. Misma causa
eventId no duplica conteos. Snapshot antiguo no sobrescribe versión nueva. Error
transitorio reintenta dos veces; cuarentena BD y DLT contienen solo referencias
topic/partición/offset y código, no payload/headers/key privados. Replay auditado de
fallos de infraestructura requiere conservar/releer el evento original desde Kafka.

Pesos recientes con decaimiento: VIEW1,SELECT2,ADD5,UPDATE3,REMOVE−5. Suma interés
propio, afinidad de categoría y cointeracciones entre productos de otros usuarios
del mismo tenant. No ML externo, azar ni lista fija. Excluye producto inexistente,
inactivo o sin stock. Estrategias PERSONALIZED, FALLBACK_POPULARITY para usuario
nuevo con popularidad y FALLBACK_CATALOG cuando aún no hay interacciones.
La recomendación usa proyección eventual; checkout revalida Productos, nunca
usa precio de recomendación como autoridad. Retención/rate-limit/ACL/TLS/lag y
hardening AWS pendientes; Kafka local PLAINTEXT no es configuración productiva.

RabbitMQ mantiene el procesamiento operativo. No reemplazarlo con Kafka ni usar
los experimentos históricos de sensores como estos servicios.

Diseño transversal en workspace: `docs/ARQUITECTURA_NUEVOS_SERVICIOS_EV3.md`.
La arquitectura inicial conserva propuestas futuras, no sustituye este contrato.
