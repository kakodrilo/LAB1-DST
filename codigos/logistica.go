//
// Logistica - dist06
//
// Programa que gestiona la recepcion de ordenes, el envio de paquetes a los camiones y el envio de informacion a finanzas
//

package main

// se importan las librerias necesarias para el funcionamiento del programa
import (
	pb "github.com/kakodrilo/LAB1-DST/pb" // libreria que contiene los compilasdos de protobuf - grpc
	"context"
	"log"
	"net"
	"google.golang.org/grpc"
	"time"
	"fmt"
	"sync"
	"os"
	"encoding/csv"
	"github.com/streadway/amqp"
	"encoding/json"
)


// estructura para el funcioanmiento del servidor para clientes
type Server struct{
	pb.UnimplementedClienteServiceServer
}

// estructura para el funcionamiento del servidor para camiones
type ServerCamiones struct{
	pb.UnimplementedCamionesServiceServer
}

// estructura principal para almacenar en memoria la informacion de los paquetes
type Paquete struct {
	timestamp string
	idpaquete int32
	tipo string
	nombre string
	valor int32
	origen string
	destino string
	seguimiento int32
	estado string
}

// estructura para almacenar infromacion que se enviara a finanzas
type PaqueteFinanzas struct{
	idpaquete int32
	estado string
	intentos int32
	valor int32
	tipo string
}

// estructura para construior el mensaje JSON
type PaqueteJson struct{
	Idpaquete int32 `json: "Idpaquete"`
	Estado string `json: "Estado"`
	Intentos int32 `json: "Intentos"`
	Valor int32 `json: "Valor"`
	Tipo string `json: "Tipo"`
}

// VARIABLES GLOBALES
// Se definen dos candados para asegurar secciones criticas
var mux sync.Mutex
var mux2 sync.Mutex

// cola que almacena los paquetes que deben ser enviados a finanzas
var cola_finanzas []PaqueteFinanzas

// tabla hashing que almacena paquetes segun su id
var ordenes map[int32]Paquete = make( map[int32]Paquete)

// tabla hashing que almacena la id de un paquete segun su codigo de seguimiento
var seguimineto_id map[int32]int32 = make(map[int32]int32)

// colas de paquetes segun tipo
var cola_retail []int32
var cola_prioritaria []int32
var cola_normal []int32

// variables para identificar paquetes
var codigo_seguimiento int32 = 0
var id_paquete int32 = 0

// se define una funcion para le manejo de errores
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}


// enviar_finanzas: envia los paquetes de la cola cola_finanzas a finanzas a través de RabbitMQ
func enviar_finanzas(){
	// se bloquea la seccion critica
	mux2.Lock()
	// se conecta con el servidor de RabbitMQ
	conn, err := amqp.Dial("amqp://test:test@10.6.40.145:5672/")
	failOnError(err, "No se puede conectar al servidor de RabbitMQ")
	defer conn.Close()
	// se instancia el canal para comunicarse
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
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
	failOnError(err, "Failed to declare a queue")
	// ciclo para enviar paquetes
	for{
		if len(cola_finanzas) > 0 { // si en la cola hay paquetes para enviar
			// se extrae el paquete
			paquete := cola_finanzas[0]
			cola_finanzas = cola_finanzas[1:]

			// se muestra por pantalla la acciona  realizar
			log.Printf("Envie a finazas el paquete %d", paquete.idpaquete)
			// se codifica el mensaje a enviar
			body, err := json.Marshal(PaqueteJson{
				Idpaquete: paquete.idpaquete,
				Estado: paquete.estado,
				Intentos: paquete.intentos,
				Valor: paquete.valor, 
				Tipo: paquete.tipo})
			if err != nil {
				log.Fatalf("No se puede convertir en JSON: %v", err)
			}
			// en pone el mensaje en la cola
			err = ch.Publish(
				"",     // exchange
				q.Name, // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "aplication/json",
					Body: body,
				})
		}
	}
	// se libera la seccion critica
	mux2.Unlock()
}

// TimeStamp: retorna un string con la fecha y hora acttual en el formato YYYY-MM-DD hh:mm
func TimeStamp () string {
	tiempo := time.Now()
	timestamp := fmt.Sprintf("%d-%02d-%02d %02d:%02d", tiempo.Year(), tiempo.Month(), tiempo.Day(), tiempo.Hour(), tiempo.Minute())
	return timestamp
}

// Se implementa la funcion para ingreasr una orden definida en el archivo proto

// IngresarOrden: reciben una orden desde el cliente, la alamcena en la cola correpondiente y registra la orden
// ctx: variable requerida para la implementacion de funciones probuf
// orden: infromacion de la orden recibida 
func (s *Server)  IngresarOrden (ctx context.Context, orden *pb.Orden) (*pb.Seguimiento, error){
	// se define la id del paquete
	id_paquete++
	// se abre archivo donde se guardara el registro
	file, err := os.OpenFile("logistica.csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()

	if err != nil {
		os.Exit(1)
	}

	// si la orden recibida es de retail
	if orden.Prioritario == 2{	
		// no tiene codigo de seguimiento
		seguimiento := &pb.Seguimiento{
			Seguimiento: 0}
	
		// se guarda en la cola retail
		cola_retail = append(cola_retail, id_paquete)
		
		// se guarda la orden en el hashing de ordenes
		ordenes[id_paquete] = Paquete{
			timestamp: TimeStamp(),
			idpaquete: id_paquete,
			tipo: "retail",
			nombre: orden.Producto,
			valor: orden.Valor,
			origen: orden.Tienda,
			destino: orden.Destino,
			seguimiento: 0,
			estado: "En Bodega"}
		
		// se registra la orden en el archivo
		x := []string{ordenes[id_paquete].timestamp, fmt.Sprint(ordenes[id_paquete].idpaquete), ordenes[id_paquete].tipo,ordenes[id_paquete].nombre,fmt.Sprint(ordenes[id_paquete].valor),ordenes[id_paquete].origen,ordenes[id_paquete].destino,fmt.Sprint(ordenes[id_paquete].seguimiento)}
		csvWriter := csv.NewWriter(file)
		strWrite := [][]string{x}
		csvWriter.WriteAll(strWrite)
		csvWriter.Flush()

		return seguimiento, nil
	}else { // en caso de que el paquete sea de pyme
		// se define el codigo de seguimiento
		codigo_seguimiento++
		// se guarda el hashing entre el codigo de seguimiento y la id del paquete
		seguimineto_id[codigo_seguimiento] = id_paquete
		// se construye el mensaje de respuesta
		seguimiento := &pb.Seguimiento{
			Seguimiento: codigo_seguimiento}
		
		if orden.Prioritario == 1{ // si la orden es prioritaria
			// se guarda en la cola prioritaria
			cola_prioritaria = append(cola_prioritaria, id_paquete)
			// se almacen al informacion en el hashing de paquetes
			ordenes[id_paquete] = Paquete{
				timestamp: TimeStamp(),
				idpaquete: id_paquete,
				tipo: "prioritario",
				nombre: orden.Producto,
				valor: orden.Valor,
				origen: orden.Tienda,
				destino: orden.Destino,
				seguimiento: codigo_seguimiento,
				estado: "En Bodega"}

		}else { // la orden es de tipo normal
			// se guarda en la cola normal
			cola_normal = append(cola_normal, id_paquete)
			// se almacena en la tabla hashing de paquetes
			ordenes[id_paquete] = Paquete{
				timestamp: TimeStamp(),
				idpaquete: id_paquete,
				tipo: "normal",
				nombre: orden.Producto,
				valor: orden.Valor,
				origen: orden.Tienda,
				destino: orden.Destino,
				seguimiento: codigo_seguimiento,
				estado: "En Bodega"}
			}
		
		// se registra la orden en el archivo
		x := []string{ordenes[id_paquete].timestamp, fmt.Sprint(ordenes[id_paquete].idpaquete), ordenes[id_paquete].tipo,ordenes[id_paquete].nombre,fmt.Sprint(ordenes[id_paquete].valor),ordenes[id_paquete].origen,ordenes[id_paquete].destino,fmt.Sprint(ordenes[id_paquete].seguimiento)}
		csvWriter := csv.NewWriter(file)
		strWrite := [][]string{x}
		csvWriter.WriteAll(strWrite)
		csvWriter.Flush()

		return seguimiento, nil
		
	}

	

}

// ConsultarEstado: Entrega el estado de una orden segun el codigo de seguimiento
// ctx: variable necesaria para la immplementacion de funciones en protobuf
// seguimiento: codigo de seguimiento
func (s *Server)  ConsultarEstado (ctx context.Context, seguimiento *pb.Seguimiento) (*pb.Estado, error){
	// se consigue la id del paquete segun el codigo de seguimiento
	id_paquete := seguimineto_id[seguimiento.Seguimiento]
	// si no se encuentra el paquete
	if id_paquete == 0 {
		return &pb.Estado{Estado: "Numero de seguimiento inexistente"}, nil
	}
	// se responde el estado del paquete
	return &pb.Estado{Estado: ordenes[id_paquete].estado}, nil
}

// ServerCliente: se da soporte al server para clientes mediante protocol buffers - grpc
func ServerClientes(){
	// se escucha mediante tcp en el puerto 9000
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("No se pudo iniciar el server: %v", err)
	}
	// se inicia el server
	s := grpc.NewServer()
	// se registra el server pora dar soporte al servicio de clientes
	pb.RegisterClienteServiceServer(s , &Server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("No se pudo crear el servidor: %v", err)
	}
}

// CambiarEstado: cambia el estado de un paquete segun la iunformacion recibida desde los camiones
// ctx: variable necesaria para implementar una funcion de protobuf
// informacion: id del paquete, estado nuevo e intentos realizados en la entrega
func (s *ServerCamiones)  CambiarEstado (ctx context.Context, informacion *pb.Informacion) (*pb.Empty, error){
	// se busca la orden para editar su estado
	orden :=  ordenes[informacion.Id]

	if orden.idpaquete == 0 {
		return &pb.Empty{}, nil
	}
	// se modifica el estado y se actualiza el hasing
	orden.estado = informacion.Estado
	ordenes[informacion.Id] = orden
	
	// se ingresa a la cola de finanzas para enviar la informacion
	cola_finanzas = append(cola_finanzas, PaqueteFinanzas{
		idpaquete: orden.idpaquete,
		estado: orden.estado,
		intentos: informacion.Intentos,
		valor: orden.valor, 
		tipo: orden.tipo})

	return &pb.Empty{}, nil

}

// SolicitarPaquete: recibe una solicitud de un camion y entrega un paquete segun le corresponda
// ctx: varible necesaria para la implementacion de funciones protobuf
// tipo: tipo del camion que esta realizando la solicitud
func (s *ServerCamiones)  SolicitarPaquete (ctx context.Context, tipo *pb.Tipo) (*pb.Paquete, error){
	// se bloquea la seccion critica
	mux.Lock()
	if tipo.Tipo == "retail" { // si el camion es tipo retail
		// se revisan las colas retail y prioritaria
		if len(cola_retail) > 0 { //  si hay paquetes en la cola retail
			// se extrae la id del paquete
			id := cola_retail[0]
			cola_retail = cola_retail[1:]
			// se extrae la informacion del apquete
			enviar := ordenes[id]
			// se comunica por pantalla que la orden es enviada 
			log.Printf("Orden %d esta En Camino", id)
			// se cambia el estado de la orden
			enviar.estado = "En Camino"
			// se actualiza el hashing
			ordenes[id] = enviar
			// se debloquea la seccion antes de responder
			mux.Unlock()
			// se envia el paquete al camion solicitante
			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		} // si la cola retail esta vacia hay que revisar la cola prioritaria
		if len(cola_prioritaria) > 0 { // si la cola prioritaria tiene paquetes
			// se extrae la id del paquete
			id := cola_prioritaria[0]
			cola_prioritaria = cola_prioritaria[1:]
			// se extrae la informacion del paquete
			enviar := ordenes[id]
			// se muestra por pantalla que el paquete esta en camino
			log.Printf("Orden %d esta En Camino", id)
			// se actualiza ewl estado de la orden y se actualiza el hashing
			enviar.estado = "En Camino"
			ordenes[id] = enviar

			mux.Unlock()
			// se envia el paquete
			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		} else { // en caso de que no hayan paquetes retail o prioritarios
			mux.Unlock()
			// se envia un paquete con id -1
			return &pb.Paquete{Id: -1}, nil
		}
	} else if tipo.Tipo == "normal" { // si el camion es de tipo normal
		if len(cola_prioritaria) > 0 { // se revisa la cola prioritaria
			// se extrae la id del paquete
			id := cola_prioritaria[0]
			cola_prioritaria = cola_prioritaria[1:]
			// se extrae la informacion del paquete
			enviar := ordenes[id]
			// se muestra por pantalla que el paquete esta en camino
			log.Printf("Orden %d esta En Camino", id)
			// se actualiuza la informacion de la orden y se actualiza la tabla hasshing
			enviar.estado = "En Camino"
			ordenes[id] = enviar

			mux.Unlock()
			// se envia la informacion del paquete
			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		}else if len(cola_normal) > 0 { // en caso de que la cola prioritaria este vacia se revisa la cola normal
			// se extrae la id del paquete
			id := cola_normal[0]
			cola_normal = cola_normal[1:]
			// se extrae la informacion del paquete
			enviar := ordenes[id]

			// se comunica que la orden esta en camino
			log.Printf("Orden %d esta En Camino", id)
			// se actualiza el estado de la orden y el hashing
			enviar.estado = "En Camino"
			ordenes[id] = enviar
			mux.Unlock()
			// se envia el paquete
			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		} else {
			mux.Unlock()
			// en caso de que no hayan paquetes prioritarios o normales se envia un paquete con id -1
			return &pb.Paquete{Id: -1}, nil
		}
	}
	mux.Unlock()
	return &pb.Paquete{Id: -1}, nil
}

// ServerCamionesInicio: da soporte al server para comunicacion con los camiones
func ServerCamionesInicio(){
	// se escucha mediante tcp en el puerto 9000
	lis, err := net.Listen("tcp", ":9001")
	if err != nil {
		log.Fatalf("No se pudo iniciar el server: %v", err)
	}
	// se inicia el server
	s := grpc.NewServer()
	// se registra el server para dar soporte al servicio de camiones
	pb.RegisterCamionesServiceServer(s , &ServerCamiones{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("No se pudo crear el servidor: %v", err)
	}
}

func main(){

	// si no existe se crea el archivo pa guardar los registros
	file, err := os.OpenFile("logistica.csv", os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()

	if err != nil {
        os.Exit(1)
	}
	// se escriben los encabezados
	x := []string{"timestamp", "id-paquete", "tipo","nombre","valor","origen","destino","seguimiento"}
    csvWriter := csv.NewWriter(file)
    strWrite := [][]string{x}
    csvWriter.WriteAll(strWrite)
	csvWriter.Flush()
	file.Close()

	// se inician los server para clientes y para camiones además de la funcion para enviar paquetes a finanzas
	go enviar_finanzas()
	go ServerClientes()
	ServerCamionesInicio()
}
