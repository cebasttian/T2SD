const express = require("express");
const cors = require("cors");
const { Kafka } = require('kafkajs')
const port = process.env.PORT;
const app = express();

app.use(cors());
app.use(express.json());

const kafka = new Kafka({
    brokers: [process.env.kafkaHost]
});

function estadisticas(registro,resultados,carros){

    if (registro.size == 0) {
		console.log("No se han registrado ventas");
	} else {

    	registro.forEach( (value, key, map) => {

        	var nVentas = 0;
        	var clientes = new Set();
        	var cantidad = 0;
        	var aux = new Array;
        	var ventasTotales = 0;
        	var promedioVentas = 0;
        	var clientesTotales = 0;
        
            value.forEach(function(data) {
                nVentas = nVentas + 1;
                clientes.add(data.cliente);
                cantidad = cantidad + parseInt(data.cantidad_sopaipillas);
            });
            
        	ventasTotales = nVentas;
        	clientesTotales = clientes.size;
        	promedioVentas = cantidad/clientesTotales;
        
        	aux.push(ventasTotales);
        	aux.push(clientesTotales);
        	aux.push(promedioVentas);
        	resultados.set(key,aux);
        	
		});
    
        resultados.forEach( (value, key, map) => {
            var aux = resultados.get(key);

            console.log("Carro : ",key);
            console.log("Ventas totales : ",aux[0]);
            console.log("Clientes totales : ",aux[1]);
            console.log("Promedio de ventas : ",aux[2]);

		});
        
        carros.clear();
        registro.clear();
        resultados.clear();
	}
}


var lista_ventas = [];
const carros = new Set();
var registro = new Map();
var resultados = new Map();

setInterval(estadisticas, 60000, registro, resultados, carros);

const ventas = async () => {
    const consumer = kafka.consumer({ groupId: 'ventas', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ventas' });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            var particion = JSON.parse(partition);

            if (message.value){
                var data = JSON.parse(message.value.toString());
                if(!carros.has(data.patente)){
                    var aux = new Array;
                    aux.push(data);
                    carros.add(data.patente);
                    registro.set(data.patente,aux);
                }else{
                    var aux = registro.get(data.patente);
                    aux.push(data);
                    registro.set(data.patente,aux);
                }
            }
        },
      })
}

const ventas_2 = async () => {
    const consumer = kafka.consumer({ groupId: 'ventas', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'ventas' });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            var particion = JSON.parse(partition);

            if (message.value){
                var data = JSON.parse(message.value.toString());
                if(!carros.has(data.patente)){
                    var aux = new Array;
                    aux.push(data);
                    carros.add(data.patente);
                    registro.set(data.patente,aux);
                }else{
                    var aux = registro.get(data.patente);
                    aux.push(data);
                    registro.set(data.patente,aux);
                }
            }
        },
      })
}


app.get("/Ventas", async (req, res) => {
    res.status(200).json({"Estadisticas": 'En ejecucion'});
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
    ventas();
    ventas_2();
});
