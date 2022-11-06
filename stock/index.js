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

var carro = new Set();
var cantidad = new Map();
var lista_stock = new Array;

const stockUno = async () => {
    const consumer = kafka.consumer({ groupId: 'stock', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'stock' });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            var particion = JSON.parse(partition);
            if(message.value){
                var data = JSON.parse(message.value.toString());
                console.log("Particion:",particion);
                
                if(carro.has(data.patente)){
                    var count = cantidad.get(data.patente);
                    count = count + 1;
                    cantidad.set(data.patente,count);
                    if(count == 5 || parseInt(data.stock_restante) < 20){
                        console.log('Carro: ',data.patente);
                        cantidad.set(data.patente,0);
                    }
                }else{
                    var count = 1;
                    carro.add(data.patente);
                    cantidad.set(data.patente,count);
                }
            }
            },
        })
  }

const stockDos = async () => {
    const consumer = kafka.consumer({ groupId: 'stock', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'stock' });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            var particion = JSON.parse(partition);
            if(message.value){
                var data = JSON.parse(message.value.toString());
                console.log("Particion:",particion);
                
                if(carro.has(data.patente)){
                    var count = cantidad.get(data.patente);
                    count = count + 1;
                    cantidad.set(data.patente,count);
                    if(count == 5 || parseInt(data.stock_restante) < 20){
                        console.log('Carro: ',data.patente);
                        cantidad.set(data.patente,0);
                    }
                }else{
                    var count = 1;
                    carro.add(data.patente);
                    cantidad.set(data.patente,count);
                }
            }
            },
        })
  }

app.get("/stock", async (req, res) => {
    res.status(200).json({"Stock": "En ejecucion"});
});


app.listen(port, () => {
    console.log(`Listening on port ${port}`);
    stockUno();
    stockDos();
});
