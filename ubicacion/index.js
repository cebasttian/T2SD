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

function coordenadas(posiciones, particion){

    posiciones.forEach((value, key, map) => {
        if(particion == 0){
            console.log("Carro",key,"coordenadas: ",value);
        }else if(particion == 1){
            console.log("Carro Profugo",key,"coordenadas: ",value);
        }
    });
}

var posiciones = new Map();
var carro = new Set();
var posicionesP = new Map();
var carroP = new Set();
var lista_posicion_carritos = [];


const avisos = async () => {
    const consumer = kafka.consumer({ groupId: 'avisos', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'avisos' });
    await consumer.run({
        partitionsConsumedConcurrently: 2,
        eachMessage: async ({ topic, partition, message }) => {
        
        var particion = JSON.parse(partition);

            if (message.value){
                if(particion == 0){
                    console.log("Particion:",particion);
                }else if(particion == 1){
                    console.log("Particion:",particion);
                }
                
                var data = JSON.parse(message.value.toString());
                
                if(particion == 0){

                    if(carroP.has(data.patente)){
                        carroP.delete(data.patente);
                    }
                    if(posicionesP.has(data.patente)){
                        posicionesP.delete(data.patente);
                    }

                    var temp = setInterval(coordenadas, 10000, posiciones, particion);

                    if(carro.has(data.patente)){
                        posiciones.set(data.patente,data.coordenadas);
                        console.log("Carro ",data.patente," coordenadas: ",data.coordenadas);
                        clearInterval(display);
                        temp = setInterval(coordenadas, 10000, posiciones, particion);

                    }else{
                        carro.add(data.patente);
                        posiciones.set(data.patente,data.coordenadas);
                        console.log("Carro ",data.patente," coordenadas: ",data.coordenadas);
                        clearInterval(display);
                        temp = setInterval(coordenadas, 10000, posiciones, particion);
                    }

                }else if(particion == 1){

                    if(carro.has(data.patente)){
                        carro.delete(data.patente);
                    }
                    if(posiciones.has(data.patente)){
                        posiciones.delete(data.patente);
                    }
                    

                    if(carroP.has(data.patente)){

                        posicionesP.set(data.patente,data.coordenadas);
                        console.log("Carro Profugo ",data.patente," coordenadas: ",data.coordenadas);
                        clearInterval(display);
                        temp = setInterval(coordenadas, 10000, posicionesP,particion);

                    }else{
                        carroP.add(data.patente);
                        posicionesP.set(data.patente,data.coordenadas);
                        console.log("Carro Profugo ",data.patente," coordenadas: ",data.coordenadas);
                        clearInterval(display);
                        temp = setInterval(coordenadas, 10000, posicionesP, particion);
                    }

                }
            }
        },
      })
}


app.get("/ubicacionCarritos", async (req, res) => {
    res.status(200).json({"coordenadas": lista_posicion_carritos});
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
    avisos();
});