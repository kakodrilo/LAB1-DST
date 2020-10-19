//
// Finanzas - dist05
//
// Programa que recibe la informacion final de los paquetes y calcula ganancias y perdidas
//

package main

// se importan las librerias necesarias para el funconamiento del codigo
import (
	"log"
	"encoding/json"
	"github.com/streadway/amqp"
	"os"
	"fmt"
	"encoding/csv"
	"os/signal"
    "syscall"
)

// se define la estructura donde se alamcenara el mensaje de RabbitMQ
type PaqueteJson struct{
	Idpaquete int32 `json: "Idpaquete"`
	Estado string `json: "Estado"`
	Intentos int32 `json: "Intentos"`
	Valor int32 `json: "Valor"`
	Tipo string `json: "Tipo"`
}
// se definen 3 variables globales para almacenar los montos requeridos
var perdida_global float32 = 0
var ganancia_global float32 = 0
var total float32 = 0

// se define una funcion para le manejo de errores
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// se abre el archivo donde se guardaran los registros de las ordenes recibidas
	file, err := os.OpenFile("finanzas.csv", os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()

	if err != nil {
        os.Exit(1)
	}
	// se escriben los encabezados de las columans del archivo creado
	x := []string{"id-paquete", "tipo","intentos","valor","estado","perdida","ganancia"}
    csvWriter := csv.NewWriter(file)
    strWrite := [][]string{x}
    csvWriter.WriteAll(strWrite)
	csvWriter.Flush()

	// se instancia la conexion con el servidor de RabbitMQ
	conn, err := amqp.Dial("amqp://test:test@10.6.40.145:5672/")
	failOnError(err, "No se puedo conectar al servidor")
	defer conn.Close()

	// se crea un canal en el servidor
	ch, err := conn.Channel()
	failOnError(err, "No se pudo crear un canal")
	defer ch.Close()
	// se declara la cola donde se almacenaran los paquetes
	q, err := ch.QueueDeclare(
		"cola", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Fallo al crear la cola de mensajes")
	// se crea una entidad que consume los mensajes de la cola
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "No se pudo crear un consumidor de mensajes")
	
	forever := make(chan bool)
	// TRABAJO DE MENSAJES RECBIDOS
	go func() {
		// se definen las variables locales para almacenar la informacion de cada paquete
		var paquete PaqueteJson
		var perdida float32
		var ganancia float32
		// por cada mensaje en la cola
		for d := range msgs {
			err = json.Unmarshal(d.Body, &paquete)
			if err != nil {
				failOnError(err,"Fallo al leer mensaje desde la cola.")
			}
			// se incia la ganancia en 0
			ganancia = 0

			if paquete.Estado == "Recibido" { // si el paquete fue recibido
				// la ganancia es igual al valor del paquete
				ganancia = float32(paquete.Valor)
			}else{ // el paquete no fue recibido
				if paquete.Tipo == "prioritario" { // si el paquete es prioritario
					// la ganancia es igual al 30 por ciento del valor
					ganancia = float32(paquete.Valor) * float32(0.3)
				}else if paquete.Tipo == "retail" {
					// la ganancia es el total del valor del paquete
					ganancia = float32(paquete.Valor)
				}
			}
			// si un paquete normal no es entregado no hay ningun tipo de ganancia

			// precio del servicio prioritario
			if paquete.Tipo == "prioritario" {
				ganancia = ganancia + float32(paquete.Valor) * float32(0.3)
			}

			// la perdida es igual al costo de reintentos por la canmtidad de reintentos
			perdida = float32(10)* float32((paquete.Intentos - int32(1)))
			
			// se suman las ganancias y perdidas
			ganancia_global = ganancia_global + ganancia
			perdida_global = perdida_global + perdida
			total = total + (ganancia - perdida)
			// se escribe el registrop del paquete
			x := []string{fmt.Sprint(paquete.Idpaquete), paquete.Tipo,fmt.Sprint(paquete.Intentos),fmt.Sprint(paquete.Valor),paquete.Estado,fmt.Sprint(perdida),fmt.Sprint(ganancia)}
			strWrite := [][]string{x}
			csvWriter.WriteAll(strWrite)
			csvWriter.Flush()
			// se muestra por pantalla los datos del paquete
			log.Printf("id: %d  intentos: %d   estado: %s    perdida: %f   ganancia: %f", paquete.Idpaquete, paquete.Intentos, paquete.Estado, perdida, ganancia)

		}
	}()
	// en caso de que se finalice el programa con CTRL + C
	exit := make(chan os.Signal)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-exit
		// se muestran los resultados de la operacion
		log.Printf("Ganacias Totales: %f", ganancia_global)
		log.Printf("Perdidas Totales: %f", perdida_global)
		log.Printf("Balance: %f", total)

		os.Exit(1)
	}()
	<-forever
}
