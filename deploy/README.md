# Despliegue propuesto, no creado

Docker multi-stage Maven/JRE 21 no root y healthcheck. CI futura test/build en PR;
publicar solo con evento autorizado, tags sha-commit y digest, escaneo/SBOM,
Actions fijadas por SHA. Sin Dockerfile/workflows activos todavía.
Imagen: `ghcr.io/zorvak749/fazbear-analytics-recommendations`.

AWS pendiente: EC2/capacidad, TG/ALB/Gateway con `/api/recomendaciones` y
`/api/analytics` y subrutas; Kafka privado TLS/SASL/ACL y grupos/DLT por dimensionar,
PostgreSQL durable/backup, SSM/IAM/SG/secretos mínimos, métricas lag/fallback/errores.
Dependencia de productores Productos/Carrito y catálogo aún por autorizar.
