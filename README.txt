
<-------- Laboratorio 1 Sistemas Distribuidos -------->

Integrantes:
    + Joaquín Castillo Tapia - 201773520-1
    + María Paz Morales - 201773505-8

-------------------------------------------------------

Dependencias instaladas:
    + dist05 - Finanzas:
        * git
        * grpc
        * RabbitMQ
    + dist06 - Logística:
        * git
        * grpc
        * RabbitMQ
    + dist07 - Clientes:
        * git
        * grpc
    + dist08 - Camiones:
        * git
        * grpc

-------------------------------------------------------

Ejecución:
    ** Se recomienda seguir el siguiente orden:

    + dist06 - Logística:
        1. Comprobar que el servidor RabbitMQ este corriendo en la maquina dist05.

        2. Dirigirse al la carpeta Logistica mediante:
                $ cd distribuidos
                $ cd Logistica

        3. Ejecutar el programa mediante:
                $ make

    + dist07 - Clientes:
        1. Asegurase que el programa en Logística está ejecutándose.

        2. Dirigirse a la carpeta Cliente mediante:
                $ cd distribuidos
                $ cd Cliente

        3. Asegurase que los archivos "retail.csv" y "pyme.csv",que contienen las ordenes a cargar, estan en la carpeta Cliente.

        4. Ejecutar el programa mediante:
                $ make

        --> Si se desea simular más de un cliente a la vez se debe abrir otra terminal y seguir los pasos 1 al 4.
	--> Para poder simular los clientes de manera ordenada, decidimos darle una sola funcionalidad a cada cliente. 
	    Entonces, primero se elige si se quiere simular tipo Pyme o Retail. Si se elige retail se ingresan los tiempos y automáticamente 
            comienza a enviar mensajes hasta finalizar. Si se elige pyme, se puede escoger entre enviar paquetes o consultar por estados, 
            si se elige el primero se preguntan por los tiempos y se empiezan a enviar los mensajes periódicamente hasta finalizar, y 
            si se elige el segundo, se comienzan a pedir códigos de seguimiento para mostrar los estados. Por lo tanto, sugerimos abrir 
            3 terminales, una que simule cada posible funcionalidad del cliente. Lo hicimos de esa manera para que la información mostrada en 
            la terminal se viera más ordenada.

    + dist05 - Finanzas:
        1. Comprobar que el servidor RabbitMQ este corriendo con el comando:
                $ /sbin/service rabbitmq-server status

        2. En caso de que el servidor no este activo encenderlo mediante:
                $ /sbin/service rabbitmq-server start

        3. Dirigirse a la carpeta Finanzas mediante:
                $ cd distribuidos
                $ cd Finanzas
        
        4. Ejecutar el programa:
                $ make

    + dist08 - Camiones:
        1. Asegurarse que el programa en Logística está ejecutándose.
    
        2. Dirigirse ala carpeta Camiones mediante:
                $ cd distribuidos
                $ cd Camiones

        3. Ejecutar el programa mediante:
                $ make


- Para finalizar la ejecucion se debe finalizar Logística, lo que lanza un error de conexion en Camiones y Cliente finalizando sus procesos.
- Para mostrar el balance total, se tiene que finalizar la ejecución del programa de Finanzas al hacer crtl+C.
- Registros generados:

    + dist06 - Logística:
        * logistica.csv : Contiene el detalle de todas las ordenes recibidas de parte de los Clientes.
    
    + dist08 - Camiones:
        * camion1.csv : Detalle de todos los paquetes atendidos por el camión 1.
        * camion2.csv : Detalle de todos los paquetes atendidos por el camión 2.
        * camion3.csv : Detalle de todos los paquetes atendidos por el camión 3.
    
    + dist05 - Finanzas:
        * finanzas.csv : Detalle finaciero de todos los paquetes recibidos desde Logística

-------------------------------------------------------

Consideraciones:
- Se asume que las ordenes con priridad 0: normal, prioridad 1: prioritario, y se asigna priridad 2 a retail.
- Se asume que el estado del pedido cambia cuando es entregado o cuando ya se hicieron todos los intentos de entrega.
- Se asume que se envía como código de seguimiento 0 a los paquetes de tipo retail.
- Se asume que el tiempo de espera de un paquete y el tiempo que toma entregar un paquete son distintos.
- Se asume que cuando se ejecutan los programas, no hay archivos (por eso se escribe el encabezado y se inicializan)
- Para no perder precisión, todos los cálculos hechos en Finanzas son representados con float32
- Cuando se finalizan los procesos de los camiones antes de que finalicen todas sus tareas, en la siguiente ejecución 
  pueden imprimir lo que les faltó al final de sus archivos. Por eso hay que tener cuidado con cortar los programas antes de tiempo.
  