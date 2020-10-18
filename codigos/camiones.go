package main

import (
	"log"
	pb "github.com/kakodrilo/LAB1-DST/pb"
	"math/rand"
	"fmt"
	//"os"
	//"encoding/csv"
	"google.golang.org/grpc"
	"time"
	"context"
)

type Paquete struct{
	id int32
	tipo string
	valor int32
	origen string
	destino string
	intentos int32
	fecha_entrega string
	intentos_max int32
}

func TimeStamp () string {
	tiempo := time.Now()
	timestamp := fmt.Sprintf("%d-%02d-%02d %02d:%02d", tiempo.Year(), tiempo.Month(), tiempo.Day(), tiempo.Hour(), tiempo.Minute())
	return timestamp
}


func SolicitarPaquete (tipo_camion string, id_camion int32, tiempo_espera int32, tiempo_entrega int32) (error){

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	conexion, err := grpc.Dial("dist06:9001", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectarse con Logistica: %v", err)
	}
	defer conexion.Close()

	camiones := pb.NewCamionesServiceClient(conexion)

	var paquetes []Paquete 

	tipo_pb := &pb.Tipo{Tipo: tipo_camion}

	for {

		for{

			res, err := camiones.SolicitarPaquete(context.Background(), tipo_pb)
			if err != nil {
				log.Fatalf("No se pueden solocitar paquetes: %v", err)
			}

			if res.Id != -1 {
				
				intentos_max := int32(3)

				if res.Tipo != "retail" {
					aux := res.Valor / int32(10)
					if aux < 3 {
						intentos_max = aux
					}
				}

				paquetes = append(paquetes, Paquete{
					id: res.Id,
					tipo: res.Tipo,
					valor: res.Valor,
					origen: res.Origen,
					destino: res.Destino,
					intentos: 0,
					fecha_entrega: "",
					intentos_max: intentos_max})

				break
			}

			time.Sleep(time.Duration(tiempo_espera)*time.Second)

		}

		res, err := camiones.SolicitarPaquete(context.Background(), tipo_pb)
		if err != nil {
			log.Fatalf("No se pueden solocitar paquetes: %v", err)
		}
		
		if res.Id == -1 {
			time.Sleep(time.Duration(tiempo_espera)*time.Second)

			res, err := camiones.SolicitarPaquete(context.Background(), tipo_pb)
			if err != nil {
				log.Fatalf("No se pueden solocitar paquetes: %v", err)
			}

			if res.Id != -1 {
				intentos_max := int32(3)

				if res.Tipo != "retail" {
					aux := res.Valor / int32(10)
					if aux < 3 {
						intentos_max = aux
					}
				}

				paquetes = append(paquetes, Paquete{
					id: res.Id,
					tipo: res.Tipo,
					valor: res.Valor,
					origen: res.Origen,
					destino: res.Destino,
					intentos: 0,
					fecha_entrega: "",
					intentos_max: intentos_max})
			}
		}else{
			intentos_max := int32(3)

			if res.Tipo != "retail" {
				aux := res.Valor / int32(10)
				if aux < 3 {
					intentos_max = aux
				}
			}

			paquetes = append(paquetes, Paquete{
				id: res.Id,
				tipo: res.Tipo,
				valor: res.Valor,
				origen: res.Origen,
				destino: res.Destino,
				intentos: 0,
				fecha_entrega: "",
				intentos_max: intentos_max})
		}
		
		if len(paquetes) == 2 {
			log.Printf("Camion %d tiene los paquetes %d y %d \n", id_camion, paquetes[0].id, paquetes[1].id)
			if paquetes[0].valor < paquetes[1].valor {
				aux := paquetes[0]
				paquetes = paquetes[1:]
				paquetes = append(paquetes, aux)
			}

			flag1 := false
			flag2 := false

			for {

				if paquetes[0].intentos < paquetes[0].intentos_max && paquetes[0].fecha_entrega == ""{
					time.Sleep(time.Duration(tiempo_entrega)*time.Second)

					eleccion := r1.Int31n(5)

					if eleccion < 4 {
						log.Printf("Camion %d entrego  paquete %d \n", id_camion, paquetes[0].id)
						paquetes[0].fecha_entrega = TimeStamp()
					}else {
						log.Printf("Camion %d intento entregar paquete %d en intento %d\n", id_camion, paquetes[0].id, paquetes[0].intentos+1)
					}
					paquetes[0].intentos++
					
				}else{
					flag1 = true
				}

				if paquetes[1].intentos < paquetes[1].intentos_max && paquetes[1].fecha_entrega == ""{
					time.Sleep(time.Duration(tiempo_entrega)*time.Second)

					eleccion := r1.Int31n(5)

					if eleccion < 4 {
						log.Printf("Camion %d entrego  paquete %d \n", id_camion, paquetes[1].id)
						paquetes[1].fecha_entrega = TimeStamp()
					}else {
						log.Printf("Camion %d intento entregar paquete %d en intento %d\n", id_camion, paquetes[1].id, paquetes[1].intentos+1)
					}
					paquetes[1].intentos++
					
				}else {
					flag2 = true
				}
				

				if flag1 && flag2 {
					break
				}

			}

			if paquetes[0].fecha_entrega == "" {
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "No Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
				paquetes[0].fecha_entrega = "0"
			}else {
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
			}

			if paquetes[1].fecha_entrega == "" {
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[1].id,
					Estado: "No Recibido",
					Intentos: paquetes[1].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
				paquetes[1].fecha_entrega = "0"
			}else {
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[1].id,
					Estado: "Recibido",
					Intentos: paquetes[1].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
			}

			paquetes = paquetes[:0]
		}else {
			log.Printf("Camion %d tiene el paquete %d \n", id_camion, paquetes[0].id)
			for {

				if paquetes[0].intentos < paquetes[0].intentos_max && paquetes[0].fecha_entrega == ""{
					time.Sleep(time.Duration(tiempo_entrega)*time.Second)

					eleccion := r1.Int31n(5)

					if eleccion < 4 {
						log.Printf("Camion %d entrego  paquete %d \n", id_camion, paquetes[0].id)
						paquetes[0].fecha_entrega = TimeStamp()
					}else {
						log.Printf("Camion %d intento entregar paquete %d en intento %d\n", id_camion, paquetes[0].id, paquetes[0].intentos+1)
					}
					paquetes[0].intentos++
					
				}else{
					break
				}
			}

			if paquetes[0].fecha_entrega == "" {
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "No Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
				paquetes[0].fecha_entrega = "0"
			}else {
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
			}
			paquetes = paquetes[:0]


		}
	}
	return nil
}

func main(){

	fmt.Println("Ingrese tiempo de espera (en segundos): ")
	var tiempo_espera int32
	fmt.Scan(&tiempo_espera)

	log.Printf("Tiempo fijado en: %d [s].", tiempo_espera)

	fmt.Println("Ingrese tiempo entre entregas (en segundos): ")
	var tiempo_entrega int32
	fmt.Scan(&tiempo_entrega)

	log.Printf("Tiempo fijado en: %d [s].", tiempo_entrega)

	go SolicitarPaquete("retail", 1, tiempo_espera , tiempo_entrega )
	go SolicitarPaquete("retail", 2, tiempo_espera , tiempo_entrega )
	SolicitarPaquete("normal", 3, tiempo_espera , tiempo_entrega )



}