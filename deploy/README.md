# Docker/CI locales e infraestructura AWS pendiente

Dockerfile multi-stage y Compose locales implementados; workflow verify ejecuta
tests, sin publicar imágenes ni desplegar. Conjunto completo y harness en
`fazbear-pago/deploy/ev3-local/`, rama `ev3/integracion-integral`.

Docker multi-stage Maven/JRE 21 no root y healthcheck. CI futura test/build en PR;
publicar solo con evento autorizado, tags sha-commit y digest, escaneo/SBOM,
Actions fijadas por SHA: hardening futuro. Dockerfile/workflow ya presentes.
Imagen: `ghcr.io/zorvak749/fazbear-analytics-recommendations`.

AWS pendiente: EC2/capacidad, TG/ALB/Gateway con `/api/recomendaciones` y
`/api/analytics` y subrutas; Kafka privado TLS/SASL/ACL y grupos/DLT por dimensionar,
PostgreSQL durable/backup, SSM/IAM/SG/secretos mínimos, métricas lag/fallback/errores.
Productores Productos/Carrito implementados y verificados en entorno local.
