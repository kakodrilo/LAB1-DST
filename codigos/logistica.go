package main

import (
	pb "github.com/kakodrilo/LAB1-DST/pb"
	"context"
	"log"
	"net"
	"google.golang.org/grpc"
	"time"
	"fmt"
)



type Server struct{
	pb.UnimplementedClienteServiceServer
}

type ServerCamiones struct{
	pb.UnimplementedCamionesServiceServer
}

type Orden struct{
	id string
	producto string
	valor int32
	tienda string
	destino string
	prioritario int32
}

type RegistroOrden struct {
	timestamp string
	idpaquete int32
	tipo string
	nombre string
	valor int32
	origen string
	destino string
	seguimiento int32
}

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

var ordenes map[int32]Paquete = make( map[int32]Paquete)

var seguimineto_id map[int32]int32 = make(map[int32]int32)

var cola_retail []int32
var cola_prioritaria []int32
var cola_normal []int32

var codigo_seguimiento int32 = 0
var id_paquete int32 = 0

func TimeStamp () string {
	tiempo := time.Now()
	timestamp := fmt.Sprintf("%d-%02d-%02d %02d:%02d", tiempo.Year(), tiempo.Month(), tiempo.Day(), tiempo.Hour(), tiempo.Minute())
	return timestamp
}

func (s *Server)  IngresarOrden (ctx context.Context, orden *pb.Orden) (*pb.Seguimiento, error){

	id_paquete++

	if orden.Prioritario == 2{	
		seguimiento := &pb.Seguimiento{
			Seguimiento: 0}

		cola_retail = append(cola_retail, id_paquete)
		
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
		

		return seguimiento, nil
	}else {
		codigo_seguimiento++

		seguimineto_id[codigo_seguimiento] = id_paquete

		seguimiento := &pb.Seguimiento{
			Seguimiento: codigo_seguimiento}
		
		if orden.Prioritario == 1{
			cola_prioritaria = append(cola_prioritaria, id_paquete)

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

		}else {
			cola_normal = append(cola_normal, id_paquete)

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

		return seguimiento, nil
		
	}

	

}

func (s *Server)  ConsultarEstado (ctx context.Context, seguimiento *pb.Seguimiento) (*pb.Estado, error){

	id_paquete := seguimineto_id[seguimiento.Seguimiento]

	if id_paquete == 0 {
		return &pb.Estado{Estado: "Numero de seguimiento inexistente"}, nil
	}

	return &pb.Estado{Estado: ordenes[id_paquete].estado}, nil

}

func ServerClientes(){
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("No se pudo iniciar el server: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterClienteServiceServer(s , &Server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("No se pudo crear el servidor: %v", err)
	}
}


func (s *ServerCamiones)  CambiarEstado (ctx context.Context, informacion *pb.Informacion) (*pb.Empty, error){

	orden :=  ordenes[informacion.Id]

	if orden.idpaquete == 0 {
		return &pb.Empty{}, nil
	}

	orden.estado = informacion.Estado

	ordenes[informacion.Id] = orden
	// NUMERO DE INTENTOS AQUI

	return &pb.Empty{}, nil

}

func (s *ServerCamiones)  SolicitarPaquete (ctx context.Context, tipo *pb.Tipo) (*pb.Paquete, error){

	if tipo.Tipo == "retail" {
		if len(cola_retail) > 0 {
			id := cola_retail[0]
			cola_retail = cola_retail[1:]

			enviar := ordenes[id]

			ordenes[id].estado = "En Camino"

			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		}else if len(cola_prioritaria) > 0 {
			id := cola_prioritaria[0]
			cola_prioritaria = cola_prioritaria[1:]

			enviar := ordenes[id]

			ordenes[id].estado = "En Camino"

			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		} else {
			return &pb.Paquete{Id: -1}, nil
		}
	} else if tipo.Tipo == "normal" {
		if len(cola_prioritaria) > 0 {
			id := cola_prioritaria[0]
			cola_retail = cola_prioritaria[1:]

			enviar := ordenes[id]

			ordenes[id].estado = "En Camino"

			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		}else if len(cola_normal) > 0 {
			id := cola_normal[0]
			cola_prioritaria = cola_normal[1:]

			enviar := ordenes[id]

			ordenes[id].estado = "En Camino"

			return &pb.Paquete{
				Id: enviar.idpaquete,
				Tipo: enviar.tipo,
				Valor: enviar.valor,
				Origen: enviar.origen,
				Destino: enviar.destino} , nil
		} else {
			return &pb.Paquete{Id: -1}, nil
		}
	}
	return &pb.Paquete{Id: -1}, nil
}


func ServerCamionesInicio(){
	lis, err := net.Listen("tcp", ":9001")
	if err != nil {
		log.Fatalf("No se pudo iniciar el server: %v", err)
	}

	s := grpc.NewServer()

	pb.RegisterCamionesServiceServer(s , &ServerCamiones{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("No se pudo crear el servidor: %v", err)
	}
}

func main(){
	ServerClientes()
	//ServerCamionesInicio()
}
