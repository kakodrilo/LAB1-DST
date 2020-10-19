package main

import (
	"log"
	"fmt"
	"os"
	"encoding/csv"
	"io"
	"strconv"
	pb "github.com/kakodrilo/LAB1-DST/pb"
	"google.golang.org/grpc"
	"time"
	"context"
)

type Orden struct{
	id string
	producto string
	valor int32
	tienda string
	destino string
	prioritario int32
}


// archivo: nombre del archivo
func CargarDatos(archivo string) ([]Orden) {
	arch, err := os.Open(archivo)
	if err != nil {
		log.Fatalln("Error al abrir el archivo %v: %v", archivo, err)
	}

	reader := csv.NewReader(arch)
	
	var ordenes []Orden

	linea, err := reader.Read() // se lee la primera linea de encabezados del archivo csv
	if err != nil {
		log.Fatalln("No se puede leer el archivo %v: %v, %v", archivo, err, linea[0])
	}

	for {
		linea, err := reader.Read()

		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln("No se puede leer el archivo %v: %v", archivo, err)
		}

		valorOrden, err :=  strconv.Atoi(linea[2])
			if err != nil {
				log.Fatalln("No se puede leer el archivo %v: %v", archivo, err)
			}
		
		if len(linea) ==  6{

			valorPrioridad, err :=  strconv.Atoi(linea[5])
			if err != nil {
				log.Fatalln("No se puede leer el archivo %v: %v", archivo, err)
			}

			orden := Orden {
				id: linea[0],
				producto: linea[1],
				valor: int32(valorOrden),
				tienda: linea[3],
				destino: linea[4],
				prioritario: int32(valorPrioridad)}
			ordenes = append(ordenes,orden)
			
		} else if len(linea) == 5{
			orden := Orden {
				id: linea[0],
				producto: linea[1],
				valor: int32(valorOrden),
				tienda: linea[3],
				destino: linea[4],
				prioritario: 2}
			ordenes = append(ordenes,orden)
		}
		
	}

	return ordenes
}

func EnviarOrdenes (ordenes []Orden, tiempo int) (error) {

	conexion, err := grpc.Dial("dist06:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectarse con Logistica: %v", err)
	}
	defer conexion.Close()

	cliente := pb.NewClienteServiceClient(conexion)

	for {
		if len(ordenes) == 0 {
			fmt.Println("No quedan ordenes para enviar")
			break
		}else {
			orden_enviar := ordenes[0]
			ordenes = ordenes[1:]

			time.Sleep(time.Duration(tiempo)*time.Second)

			orden_enviar_pb := &pb.Orden{
				Id: orden_enviar.id,
				Producto: orden_enviar.producto,
				Valor: orden_enviar.valor,
				Tienda: orden_enviar.tienda,
				Destino: orden_enviar.destino,
				Prioritario: orden_enviar.prioritario}

			res, err := cliente.IngresarOrden(context.Background(), orden_enviar_pb)

			if err != nil {
				log.Fatalf("No se puede enviar la orden: %v", err)
			}
			if res.Seguimiento != 0 {
				log.Printf("Codigo de Seguimiento para %s : %d", orden_enviar.id, res.Seguimiento)
			}else {
				log.Printf("Se ha enviado la orden %s de retail", orden_enviar.id)
			}
			
		}
	}

	return nil
}

func ConsultarEstado () (error){

	conexion, err := grpc.Dial("dist06:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectarse con Logistica: %v", err)
	}
	defer conexion.Close()

	cliente := pb.NewClienteServiceClient(conexion)

	for {
		log.Printf("Ingrese numero de seguimiento (-1 para salir): ")
		var numero_segumiento int32
		fmt.Scan(&numero_segumiento)
		if numero_segumiento == -1 {
			break
		}

		seguimiento := &pb.Seguimiento{Seguimiento: numero_segumiento}

		res, err := cliente.ConsultarEstado(context.Background(), seguimiento)

		if err != nil {
			log.Fatalf("No se puede consultar: %v", err)
		}

		log.Printf("Estado del paquete para la orden %d : %s", numero_segumiento, res.Estado)
	}
	return nil

}


func main(){

	fmt.Println("Seleccione el tipo de cliente a emular:")
	fmt.Println("[1] Pyme      [2] Retail")
	
	log.Printf("\nOpcion: ")
	var opcion int
	fmt.Scan(&opcion)

	var ordenes []Orden 
	var tiempo int

	if opcion == 2 {
		ordenes = CargarDatos("retail.csv")
		fmt.Println("\nDatos correctamente cargados\n")
		
		log.Printf("Ingrese frecuencia de envio de ordenes (en segundos): ")
		fmt.Scan(&tiempo)
		log.Printf("Tiempo fijado en: %d [s].", tiempo)

		EnviarOrdenes(ordenes, tiempo)
		
	} else if opcion == 1 {

		fmt.Println("Seleccione accion:")
		fmt.Println("[1] Enviar Ordenes       [2] Consultar Estado")
		
		log.Printf("\nOpcion: ")

		
		fmt.Scan(&opcion)

		fmt.Println("-> Iniciando carga de ordenes <-")

		if opcion == 1{
			ordenes = CargarDatos("pymes.csv")
			fmt.Println("\nDatos correctamente cargados\n")
		
			log.Printf("Ingrese frecuencia de envio de ordenes (en segundos): ")
			fmt.Scan(&tiempo)

			log.Printf("Tiempo fijado en: %d [s].", tiempo)

			EnviarOrdenes(ordenes, tiempo)
		}else if opcion == 2{
			ConsultarEstado()
		}

		
	}
}