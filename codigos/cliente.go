//
// Cliente - dist07
//
// Programa que simula el ingreso de ordenes y permite la consulta de paquetes 
//

package main

// se importan la librerias necesarias para el funcionamiento
import (
	"log"
	"fmt"
	"os"
	"encoding/csv"
	"io"
	"strconv"
	pb "github.com/kakodrilo/LAB1-DST/pb"  // libreria que contiene los compilados de probuf - grpc
	"google.golang.org/grpc"
	"time"
	"context"
)

// se define la estructura orden para almacenar los datos ingresados en memoria
type Orden struct{
	id string
	producto string
	valor int32
	tienda string
	destino string
	prioritario int32
}

// CargarDatos: recibe el nombre de un archivo para cargar los datos. Devuelve un arreglo con los datos.
// archivo string: nombre del archivo
func CargarDatos(archivo string) ([]Orden) {

	// se abre el archivo y se finaliza el programa si es que el archivo no existe o no se puede abrir
	arch, err := os.Open(archivo)
	if err != nil {
		log.Fatalln("Error al abrir el archivo %v: %v", archivo, err)
	}

	reader := csv.NewReader(arch)
	
	// se crea el arreglo ordenes donde se guardaran los datos
	var ordenes []Orden

	linea, err := reader.Read() // se lee la primera linea de encabezados del archivo csv
	if err != nil {
		log.Fatalln("No se puede leer el archivo %v: %v, %v", archivo, err, linea[0])
	}

	// se inicia un ciclo para leer todas las lineas del archivo
	for {
		// se lee una linea
		linea, err := reader.Read()

		// condicion de salida
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln("No se puede leer el archivo %v: %v", archivo, err)
		}

		// se transforma el string valor a un entero
		valorOrden, err :=  strconv.Atoi(linea[2])
		
		// se diferencia entre retail o pyme a traves de la existencia de la prioridad
		if len(linea) ==  6{ // es pyme

			// se transforma la prioridad a un string
			valorPrioridad, err :=  strconv.Atoi(linea[5])
			if err != nil {
				log.Fatalln("No se puede leer el archivo %v: %v", archivo, err)
			}
			// se crea la orden
			orden := Orden {
				id: linea[0],
				producto: linea[1],
				valor: int32(valorOrden),
				tienda: linea[3],
				destino: linea[4],
				prioritario: int32(valorPrioridad)}
			// se almacena la orden en el arreglo
			ordenes = append(ordenes,orden)
			
		} else if len(linea) == 5{ // es de retail
			// se crea la orden con prioridad 2
			orden := Orden {
				id: linea[0],
				producto: linea[1],
				valor: int32(valorOrden),
				tienda: linea[3],
				destino: linea[4],
				prioritario: 2}
			// se agrega la orden a la lista
			ordenes = append(ordenes,orden)
		}
		
	}
	// se retorna el arreglo con todas las ordenes cargadas
	return ordenes
}


// EnviarOrdenes: recibe un arreglo de ordenes que envia cada cierto tiempo
// ordenes: arreglo con las ordenes a enviar
// tiempo: intervalo de tiempo en segundos
func EnviarOrdenes (ordenes []Orden, tiempo int) (error) {

	// se establece la conexion con logistica a traves de grpc
	conexion, err := grpc.Dial("dist06:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectarse con Logistica: %v", err)
	}
	defer conexion.Close()

	// se instancia el cliente definido en protocolbuffers
	cliente := pb.NewClienteServiceClient(conexion)

	// se recorre el arreglo de ordenes 
	for {
		// condicion de salida
		if len(ordenes) == 0 {
			fmt.Println("No quedan ordenes para enviar")
			break
		}else {
			// se extrae la primera orden del arreglo
			orden_enviar := ordenes[0]
			ordenes = ordenes[1:]
			// se espera un tiempo definido antes de enviar la orden
			time.Sleep(time.Duration(tiempo)*time.Second)

			// se crea la orden a enviar
			orden_enviar_pb := &pb.Orden{
				Id: orden_enviar.id,
				Producto: orden_enviar.producto,
				Valor: orden_enviar.valor,
				Tienda: orden_enviar.tienda,
				Destino: orden_enviar.destino,
				Prioritario: orden_enviar.prioritario}

			// se envia la orden
			res, err := cliente.IngresarOrden(context.Background(), orden_enviar_pb)

			if err != nil {
				log.Fatalf("No se puede enviar la orden: %v", err)
			}
			// se muestra por pantalla la respuesta segun el tipo de cliente
			if res.Seguimiento != 0 { // clientes pyme
				log.Printf("Codigo de Seguimiento para %s : %d", orden_enviar.id, res.Seguimiento)
			}else { // clientes retail
				log.Printf("Se ha enviado la orden %s de retail", orden_enviar.id)
			}
			
		}
	}

	return nil
}

// ConsultarEstado: consulta el estado de una orden segun el codigo de seguimiento
func ConsultarEstado () (error){

	// se establece la conexion con logistica
	conexion, err := grpc.Dial("dist06:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectarse con Logistica: %v", err)
	}
	defer conexion.Close()

		// se instancia un tipo cliente segun lo definido en protocolbuffers
	cliente := pb.NewClienteServiceClient(conexion)

	// se crea un ciclo para preguntar constantemente por alguna orden
	for {
		// se solicita por pantalla el numero de seguimiento
		log.Printf("Ingrese numero de seguimiento (-1 para salir): ")
		var numero_segumiento int32
		fmt.Scan(&numero_segumiento)
		// condicion de salida
		if numero_segumiento == -1 {
			break
		}
		// se crea el mensaje para consultar por el estado
		seguimiento := &pb.Seguimiento{Seguimiento: numero_segumiento}
		// se consulta el estado
		res, err := cliente.ConsultarEstado(context.Background(), seguimiento)

		if err != nil {
			log.Fatalf("No se puede consultar: %v", err)
		}
		// se muestra por pantalla la respuesta recibida
		log.Printf("Estado del paquete para la orden %d : %s", numero_segumiento, res.Estado)
	}
	return nil

}


func main(){
	// se crea un menu en la terminal para la interaccion con el usuario
	fmt.Println("Seleccione el tipo de cliente a emular:")
	fmt.Println("[1] Pyme      [2] Retail")
	
	log.Printf("\nOpcion: ")
	var opcion int
	fmt.Scan(&opcion)

	var ordenes []Orden 
	var tiempo int

	// manejo de opciones
	if opcion == 2 { // cliente tipo retail solo puede enviar ordenes
		// se cargan los datos
		ordenes = CargarDatos("retail.csv") 
		fmt.Println("\nDatos correctamente cargados\n")
		// se pregunta el intervalo de tiempo en segundos
		log.Printf("Ingrese frecuencia de envio de ordenes (en segundos): ")
		fmt.Scan(&tiempo)
		log.Printf("Tiempo fijado en: %d [s].", tiempo)
		// se envian las ordenes cargadas cada cierto tiempo entregado
		EnviarOrdenes(ordenes, tiempo)
		
	} else if opcion == 1 { // cliente pyme puede enviar ordenes o consultar estado de una orden

		fmt.Println("Seleccione accion:")
		fmt.Println("[1] Enviar Ordenes       [2] Consultar Estado")
		
		log.Printf("\nOpcion: ")

		
		fmt.Scan(&opcion)

		fmt.Println("-> Iniciando carga de ordenes <-")

		if opcion == 1{ // si el cleinte pyme enviara ordenes
			// se cargan los datos
			ordenes = CargarDatos("pymes.csv")
			fmt.Println("\nDatos correctamente cargados\n")
			// se consulta el intervalo de timepo deseado
			log.Printf("Ingrese frecuencia de envio de ordenes (en segundos): ")
			fmt.Scan(&tiempo)
			log.Printf("Tiempo fijado en: %d [s].", tiempo)
			// se envian las ordenes
			EnviarOrdenes(ordenes, tiempo)
		}else if opcion == 2{ // si el cliente pyme consultara estados de ordenes
			ConsultarEstado()
		}

		
	}
}