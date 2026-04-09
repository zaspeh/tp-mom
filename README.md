# Trabajo Práctico - Middlewares Orientados a Mensajes

Los middlewares orientados a mensajes (MOMs) son un recurso importante para el control de la complejidad en los sistemas distribuídos, puesto que permiten a las distintas partes del sistema comunicarse abstrayéndose de problemas como los cambios de ubicación, fallos, performance y escalabilidad.

En este repositorio se proveen conjuntos de pruebas para los dos formas más comunes de organización de la comunicación sobre colas, que en RabbitMQ se denominan Work Queues y Exchanges.

Se recomienda familiarizarse con estos conceptos leyendo la documentación de RabbitMQ y siguiendo los [tutoriales introductorios](https://www.rabbitmq.com/tutorials).

## Condiciones de Entrega

El código de este repositorio se agrupa en dos carpetas, una para Python y otra para Golang. Los estudiantes deberán elegir **sólo uno** de estos lenguajes y completar la implementación de las interfaces de middleware provistas con el objetivo de pasar las pruebas asociadas.

Al momento de la evaluación y ejecución de las pruebas se **descartarán** los cambios realizados a todos los archivos, a excepción de:

**Python:** `/python/src/common/middleware/middleware_rabbitmq.py` 

**Golang:** `/golang/internal/factory/*/*.go` 

## Ejecución

`make up` : Inicia contenedores de RabbitMQ  y de pruebas de integración. Comienza a seguir los logs de las pruebas.

`make down`:   Detiene los contenedores de pruebas y destruye los recursos asociados.

`make logs`: Sigue los logs de todos los contenedores en un solo flujo de salida.

`make local`: Ejecuta las pruebas de integración desde el Host, facilitando el desarrollo. Se explica con mayor detalle dentro de su sección.

## Pruebas locales desde el Host

Habiendo iniciado el contenedor de RabbitMQ o configurado una instancia local del mismo pueden ejecutarse las pruebas sin necesidad de detener y reiniciar los contenedores ejecutando `make local`, siempre que se cumplan los siguientes requisitos.

### Python
Instalar una versión de Python superior a `3.14`. Se recomienda emplear un gestor de versiones, como ser `pyenv`.
Instalar los dependencias de la suite de pruebas:
`pip install -r python/src/tests/requirements.txt`

### Golang
Instalar una versión de Golang superior a `1.24`.
Instalar los dependencias de la suite de pruebas:
`go mod download`

Algunas aclaraciones:
* A la hora de enviar mensajes, el contexto permite un máximo de 5 segundos para poder ejecutar 'PublishWitchContext'. (se decidieron 5 segundos debido a que en la documentación de RabbitMQ donde se utilizan 5 segundos también.)
* Al conectarse con RabbitMQ 'ampq.Dial(...)', reintenta conectarse 10 (esto está fijado en la variable global TRIES_TO_CONNECT) veces cada 2 segundos antes de lanzar error por desconexión.
* Si StartConsuming es invocado más de una vez sobre la misma instancia, el consumerTag anterior se sobrescribe, lo que implica que no podrá cancelarse posteriormente mediante StopConsuming. Este comportamiento no está controlado actualmente y se asume un único consumidor por instancia.
* Las colas y exchanges se configuran con durability = false, lo que implica que no se persisten en disco. Esto fue decidido asumiendo que la persistencia de mensajes no es un requerimiento.
