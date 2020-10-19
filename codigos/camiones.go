//
// Camiones - dist08
//
// Programa que simula el funcionamiento de los 3 camiones de entregas
//

package main


// se importan la librerias necesarias para el funcionamiento del codigo
import (
	"log"
	pb "github.com/kakodrilo/LAB1-DST/pb" // libreria que contiene los compilados de protobuf - grpc
	"math/rand"
	"fmt"
	"os"
	"encoding/csv"
	"google.golang.org/grpc"
	"time"
	"context"
)

// se define la estructura Paquete que almacena los datos necesarios de cada paquete
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

// TimeStamp: retorna un string con la fecha y hora acttual en el formato YYYY-MM-DD hh:mm
func TimeStamp () string {
	tiempo := time.Now()
	timestamp := fmt.Sprintf("%d-%02d-%02d %02d:%02d", tiempo.Year(), tiempo.Month(), tiempo.Day(), tiempo.Hour(), tiempo.Minute())
	return timestamp
}

// SolicitarPaquete: funcionamiento completo de cada camion: solicitud de ordenes, entrega, cambio de estados y registro de paquetes finalizados
// tipo_camion: retail o normal
// id_camion: identificador de cada camion
// tiempo_espera: tiempo de espera entre solicitud de ordenes
// tiempo_entrega: tiempo en que se demora en realizar una entrega
func SolicitarPaquete (tipo_camion string, id_camion int32, tiempo_espera int32, tiempo_entrega int32) (error){

	// se define una nueva fuente para los numeros random
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	// se establece conexion con logistica
	conexion, err := grpc.Dial("dist06:9001", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Error al conectarse con Logistica: %v", err)
	}
	defer conexion.Close()
	// se instancia un nuevo cliente segun lo definido en protocolbuffers
	camiones := pb.NewCamionesServiceClient(conexion)

	// arreglo para almacenar los paquetes a entregar
	var paquetes []Paquete 
	// se instancia el mensaje para solicitud de paquetes
	tipo_pb := &pb.Tipo{Tipo: tipo_camion}
	// se crea el archivo para guardar los datos de cada entrega
	file, err := os.OpenFile("camion"+fmt.Sprint(id_camion)+".csv", os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()

	if err != nil {
        os.Exit(1)
	}
	// se escribe el encabezado del archivo
	x := []string{"id-paquete", "tipo", "valor","origen","destino","intentos","fecha-entrega"}
	var y []string
	csvWriter := csv.NewWriter(file)
    strWrite := [][]string{x}
    csvWriter.WriteAll(strWrite)
	csvWriter.Flush()

	// se incia el ciclo de funcionamiento del camion
	for {
		// se soilictan paquetes hasta recibir al menos 1
		for{
			// se consulta por paquetes disponibles a logistica
			res, err := camiones.SolicitarPaquete(context.Background(), tipo_pb)
			if err != nil {
				log.Fatalf("No se pueden solocitar paquetes: %v", err)
			}
			
			if res.Id != -1 { // en caso de que se reciba un paquete
				// se establece la cantidad de intentos maximos segun tipo de paquete y/o su valor
				intentos_max := int32(3)

				if res.Tipo != "retail" {
					aux := (res.Valor + int32(10)) / int32(10)
					if aux < 3{
						intentos_max = aux
					}
				}
				// se agrega el paquete a ql arreglo de paquetes
				paquetes = append(paquetes, Paquete{
					id: res.Id,
					tipo: res.Tipo,
					valor: res.Valor,
					origen: res.Origen,
					destino: res.Destino,
					intentos: 0,
					fecha_entrega: "",
					intentos_max: intentos_max})

				break // se termina el ciclo, ya se tiene al menos 1 paquete
			}
			// en caso de que no hayan paquetes disponibles se espera antes de volver a solicitar
			time.Sleep(time.Duration(tiempo_espera)*time.Second)

		}
		// se consulta por un segundo paquete
		res, err := camiones.SolicitarPaquete(context.Background(), tipo_pb)
		if err != nil {
			log.Fatalf("No se pueden solocitar paquetes: %v", err)
		}
		
		if res.Id == -1 { // no hay paquetes disponibles
			// se espera un  tiempo definido antes de volver a solicitar un paquete
			time.Sleep(time.Duration(tiempo_espera)*time.Second)

			res, err := camiones.SolicitarPaquete(context.Background(), tipo_pb)
			if err != nil {
				log.Fatalf("No se pueden solocitar paquetes: %v", err)
			}

			if res.Id != -1 { // si se recibe un paquete
				// se establece la cantidad de intentos maximos
				intentos_max := int32(3)

				if res.Tipo != "retail" {
					aux := (res.Valor + int32(10)) / int32(10)
					if aux < 3{
						intentos_max = aux
					}
				}
				// se ingresa el paquete al arerglo de paquetes a entregar
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
		}else{ // en caso de que si había otro paquete disponible no hay que esperar
			// se establece la cantidad de intentos maximos
			intentos_max := int32(3)

			if res.Tipo != "retail" {
				aux := (res.Valor + int32(10)) / int32(10)
				if aux < 3{
					intentos_max = aux
				}
			}
			// se ingresa el paquete en el arreglo de paquetes a entregar
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
		// ENTREGA DE PAQUETES
		if len(paquetes) == 2 { // son dos los paquetes a entregar
			// se muestra por terminal que paquetes tiene el camion
			log.Printf("Camion %d tiene los paquetes %d y %d \n", id_camion, paquetes[0].id, paquetes[1].id)
			// se priorizan los paquetes segun el valor de cada uno
			if paquetes[0].valor < paquetes[1].valor {
				aux := paquetes[0]
				paquetes = paquetes[1:]
				paquetes = append(paquetes, aux)
			}
			// se definen variables apra saber cuando finalizar la entrega
			flag1 := false
			flag2 := false
			// inicia un ciclo para entregar los paquetes
			for {
				// si el primer paquete aun no ha sido entregado y aun quedan intentos
				if paquetes[0].intentos < paquetes[0].intentos_max && paquetes[0].fecha_entrega == ""{
					// se espera un tiempo determinado para simular el trayecto del camion
					time.Sleep(time.Duration(tiempo_entrega)*time.Second)
					// se escoge un numero random entre el 0 y el 4
					eleccion := r1.Int31n(5)

					if eleccion < 4 { // si la eleccion es 0, 1, 2 o 3 (80 porciento de prob)
						// se muestra por terminal que el paquete ha sido entregado
						log.Printf("Camion %d entrego  paquete %d \n", id_camion, paquetes[0].id)
						// se establece la fecha de entrega
						paquetes[0].fecha_entrega = TimeStamp()
					}else { // la entrega fallo
						// se comunica por pantalla el intento de entrega
						log.Printf("Camion %d intento entregar paquete %d en intento %d\n", id_camion, paquetes[0].id, paquetes[0].intentos+1)
					}
					// se aumenta la cantidad de intentos del paquete
					paquetes[0].intentos++
					
				}else{ // el paquete ya ha sido entregado o no quedan intentos
					flag1 = true
				}
				// si el segundo paquete aun no ha sido entregado y aun quedan intentos
				if paquetes[1].intentos < paquetes[1].intentos_max && paquetes[1].fecha_entrega == ""{
					// se espera un tiempo determinado para simular el trayecto
					time.Sleep(time.Duration(tiempo_entrega)*time.Second)
					// se escoge un numero random entre 0 y 4
					eleccion := r1.Int31n(5)

					if eleccion < 4 { // si la entrega es exitosa
						// se comunica por pantalla que el paquete fue entregado
						log.Printf("Camion %d entrego  paquete %d \n", id_camion, paquetes[1].id)
						// se establece la fecha de entrega
						paquetes[1].fecha_entrega = TimeStamp()
					}else { // si la entrega falla
						// se comunica por pantalla el intento de entrega
						log.Printf("Camion %d intento entregar paquete %d en intento %d\n", id_camion, paquetes[1].id, paquetes[1].intentos+1)
					}
					// se aumeta el numero de intentos
					paquetes[1].intentos++
					
				}else { // el paquete ya ha sido entregado o no quedan intentos
					flag2 = true
				}
				
				// en caso de que ambos paquetes ya se entregaron o no quedan intentos se termina el ciclo
				if flag1 && flag2 {
					break
				}

			}
			// COMUNICAR A LOGISTICA EL RESULTADO DE LA RUTA
			if paquetes[0].fecha_entrega == "" { // si el primer paquete no fue recibido
				// se comunica al logistica el resultado con los intentos realizados
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "No Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se puede cambiar el estado: %v", err)
				}
				// se establece la fecha de entrega en 0 para el registro
				paquetes[0].fecha_entrega = "0"
			}else { // el primer paquete fue recibido
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se puede cambiar el estado: %v", err)
				}
			}

			if paquetes[1].fecha_entrega == "" { // el segundo paquete no fue recibido
				// se comunica a logistica el resultado del la entrega
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[1].id,
					Estado: "No Recibido",
					Intentos: paquetes[1].intentos})
				if err != nil {
					log.Fatalf("No se puede cambiar el estado: %v", err)
				}
				// se establece la fecha de entrega en 0 para su psoterior registro
				paquetes[1].fecha_entrega = "0"
			}else { // el paquete fue recibido 
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[1].id,
					Estado: "Recibido",
					Intentos: paquetes[1].intentos})
				if err != nil {
					log.Fatalf("No se puede cambiar el estado: %v", err)
				}
			}
			// se escribe el registro del resultado de la ruta
			x = []string{fmt.Sprint(paquetes[0].id), paquetes[0].tipo, fmt.Sprint(paquetes[0].valor),paquetes[0].origen,paquetes[0].destino,fmt.Sprint(paquetes[0].intentos),paquetes[0].fecha_entrega}
			y = []string{fmt.Sprint(paquetes[1].id), paquetes[1].tipo, fmt.Sprint(paquetes[1].valor),paquetes[1].origen,paquetes[1].destino,fmt.Sprint(paquetes[1].intentos),paquetes[1].fecha_entrega}
			strWrite = [][]string{x,y}
			csvWriter.WriteAll(strWrite)
			csvWriter.Flush()
			// re reinicia el arreglo de paquetes a entregar
			paquetes = paquetes[:0]

		}else { // en caso de que solo se tenga 1 paquete para entregar
			// se muetra por pantalla que el camion tiene el paquete
			log.Printf("Camion %d tiene el paquete %d \n", id_camion, paquetes[0].id)
			// ciclo para representar la entrega
			for {
				// si el paquete no ha sido entregado y aun quedan intentos
				if paquetes[0].intentos < paquetes[0].intentos_max && paquetes[0].fecha_entrega == ""{
					// se espéra un tiempo determinado para representar el trayecto
					time.Sleep(time.Duration(tiempo_entrega)*time.Second)
					// se escoge un numero random entre 0 y 4
					eleccion := r1.Int31n(5)

					if eleccion < 4 { // el paquete fue recibido
						// se comunica por pantalla que el paquete fue entregado
						log.Printf("Camion %d entrego  paquete %d \n", id_camion, paquetes[0].id)
						// se establece la fecha de entrega
						paquetes[0].fecha_entrega = TimeStamp()
					}else { // fallo el intento
						log.Printf("Camion %d intento entregar paquete %d en intento %d\n", id_camion, paquetes[0].id, paquetes[0].intentos+1)
					}
					// se aumenta la cantidad de intentos del paquete
					paquetes[0].intentos++
					
				}else{ // el paquete ya fue entregado o no quedan intentos
					break
				}
			}
			// si el paquete no fue recibido
			if paquetes[0].fecha_entrega == "" {
				// se comunica a logistica el resultado de la entrega
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "No Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
				paquetes[0].fecha_entrega = "0"
			}else { // si el paquete fue recibido
				// se comunica a logistica el resultado de la entrega
				_, err := camiones.CambiarEstado(context.Background(), &pb.Informacion{
					Id: paquetes[0].id,
					Estado: "Recibido",
					Intentos: paquetes[0].intentos})
				if err != nil {
					log.Fatalf("No se pueden solocitar paquetes: %v", err)
				}
			}
			// se escribe en el registro el resultado de la entrega y la informacion del paquete
			x = []string{fmt.Sprint(paquetes[0].id), paquetes[0].tipo, fmt.Sprint(paquetes[0].valor),paquetes[0].origen,paquetes[0].destino,fmt.Sprint(paquetes[0].intentos),paquetes[0].fecha_entrega}
			strWrite = [][]string{x}
			csvWriter.WriteAll(strWrite)
			csvWriter.Flush()
			// se reinicia el registro de los paquetes
			paquetes = paquetes[:0]

		}
	}
	return nil
}

func main(){

	// se pregunta el tiempo de espera entre solicitud de poquetes
	fmt.Println("Ingrese tiempo de espera (en segundos): ")
	var tiempo_espera int32
	fmt.Scan(&tiempo_espera)

	log.Printf("Tiempo fijado en: %d [s].", tiempo_espera)
	// se consulta el tiempo de trayecto de los camiones
	fmt.Println("Ingrese tiempo entre entregas (en segundos): ")
	var tiempo_entrega int32
	fmt.Scan(&tiempo_entrega)

	log.Printf("Tiempo fijado en: %d [s].", tiempo_entrega)

	// se inicia el funcionamiento de los 3 camiones: 2 retail y 1 normal cada uno en hebra separada
	go SolicitarPaquete("retail", 1, tiempo_espera , tiempo_entrega )
	go SolicitarPaquete("retail", 2, tiempo_espera , tiempo_entrega )
	SolicitarPaquete("normal", 3, tiempo_espera , tiempo_entrega )



}