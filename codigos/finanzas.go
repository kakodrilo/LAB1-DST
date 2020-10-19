package main

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


type PaqueteJson struct{
	Idpaquete int32 `json: "Idpaquete"`
	Estado string `json: "Estado"`
	Intentos int32 `json: "Intentos"`
	Valor int32 `json: "Valor"`
	Tipo string `json: "Tipo"`
}

var perdida_global float32 = 0
var ganancia_global float32 = 0
var total float32 = 0


func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {

	file, err := os.OpenFile("finanzas.csv", os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()

	if err != nil {
        os.Exit(1)
	}

	x := []string{"id-paquete", "tipo","intentos","valor","estado","perdida","ganancia"}
    csvWriter := csv.NewWriter(file)
    strWrite := [][]string{x}
    csvWriter.WriteAll(strWrite)
	csvWriter.Flush()

	conn, err := amqp.Dial("amqp://test:test@10.6.40.145:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"cola", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	
	forever := make(chan bool)

	go func() {
		var paquete PaqueteJson
		var perdida float32
		var ganancia float32
		for d := range msgs {
			err = json.Unmarshal(d.Body, &paquete)
			if err != nil {
				failOnError(err,"Fallo al leer mensaje desde la cola.")
			}
			ganancia = 0

			if paquete.Estado == "Recibido" {
				ganancia = float32(paquete.Valor)
			}else{
				if paquete.Tipo == "prioritario" {
					ganancia = float32(paquete.Valor) * 0.3
				}else if paquete.Tipo == "retail" {
					ganancia = float32(paquete.Valor)
				}
			}

			perdida = float32(10)* float32((paquete.Intentos - int32(1)))
			
			ganancia_global = ganancia_global + ganancia
			perdida_global = perdida_global + perdida
			total = total + (ganancia - perdida)
			x := []string{fmt.Sprint(paquete.Idpaquete), paquete.Tipo,fmt.Sprint(paquete.Intentos),fmt.Sprint(paquete.Valor),paquete.Estado,fmt.Sprint(perdida),fmt.Sprint(ganancia)}
			strWrite := [][]string{x}
			csvWriter.WriteAll(strWrite)
			csvWriter.Flush()

			log.Printf("id: %d  intentos: %d   estado: %s    perdida: %f   ganancia: %f", paquete.Idpaquete, paquete.Intentos, paquete.Estado, perdida, ganancia)

		}
	}()
	exit := make(chan os.Signal)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-exit
		log.Printf("Ganacias Totales: %f", ganancia_global)
		log.Printf("Perdidas Totales: %f", perdida_global)
		log.Printf("Balance: %f", total)

		os.Exit(1)
	}()
	<-forever
}
