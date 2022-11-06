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

var white_list = new Map();
var nuevos_miembros = [];


const ingreso = async () => {
    const consumer = kafka.consumer({ groupId: 'nuevos_miembros', fromBeginning: true });
    await consumer.connect();
    await consumer.subscribe({ topic: 'nuevos_miembros' });
    await consumer.run({
        partitionsConsumedConcurrently: 2,
        eachMessage: async ({ topic, partition, message }) => {
            var tipo = JSON.parse(partition);

            if (message.value){
                var data = JSON.parse(message.value.toString());
                nuevos_miembros.push(data);
                if(tipo==0){
                    console.log("Tipo:",tipo,"normal");
                    console.log("Se ha registrado un nuevo miembro");
                }else if(tipo==1){
                    console.log("Tipo:",tipo,"premium");
                    console.log("Se ha registrado un nuevo miembro premium");
                }
                
            }
        },
      })
}

app.get("/ingresos", async (req, res) => {
    res.status(200).json({"nuevos_miembros": nuevos_miembros});
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
    ingreso();
});