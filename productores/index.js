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

const producer = kafka.producer();

const topicParticiones  = async () => {
    const admin = kafka.admin();
    await admin.connect();
    await admin.createTopics({
        topics: [
        	{ topic: 'ingreso', numPartitions: 2 },
        	{ topic: 'ventas',  numPartitions: 2 },
        	{ topic: 'stock',   numPartitions: 2 },
        	{ topic: 'avisos',  numPartitions: 2 }
        ],
});
    await admin.disconnect();
};

topicParticiones().then(() => console.log('done'));

function sleep(ms){
    return new Promise(resolve => setTimeout(resolve, ms));
}

var carros = new Set();

app.post("/RegistroMiembro", async (req, res) => {
    req.body.time = new Date().getTime();
    await producer.connect();

    if(!carros.has(req.body.patente)){

        carros.add(req.body.patente);
        if(req.body.premium){
            await producer.send({
                topic: 'ingreso',    
                messages: [{value: JSON.stringify(req.body),partition:1}]
            })
        }else{
            await sleep(5000);
            await producer.send({
                topic: 'ingreso',   
                messages: [{value: JSON.stringify(req.body),partition:0}]
            })
        }

        await producer.disconnect().then(
            res.status(200).json({
                nombre: req.body.nombre,
                patente: req.body.patente
            })
        )
    }else{
        await producer.disconnect().then(
            res.status(200).json({
                error: "El carro ya existe."
            })
        )
    }
});

app.post("/RegistroVenta", async (req, res) => {

    req.body.time = new Date().getTime();
    await producer.connect();

    if(carros.has(req.body.patente)){
        await producer.send({
            topic: 'ventas',                             
            messages: [{value: JSON.stringify(req.body)}]
        })
        await producer.send({
            topic: 'stock',                                
            messages: [{value: JSON.stringify(req.body)}]
        })
        await producer.send({
            topic: 'avisos',                           
            messages: [{value: JSON.stringify(req.body),partition:0}]
        })
        await producer.disconnect().then(
            res.status(200).json({
                cliente: req.body.cliente,
                coordenadas: req.body.coordenadas
            })
        )
    }else{
        await producer.disconnect().then(
            res.status(200).json({
                error: "El carro no existe."
            })
        )
    }
        
    
});

app.post("/RegistroAviso", async (req, res) => {
   await producer.connect();
    if(carros.has(req.body.patente)){
    await producer.send({
        topic: 'avisos',                           
        messages: [{value: JSON.stringify(req.body),partition:1}]
    })
    await producer.disconnect().then(
        res.status(200).json({
            coordenadas: req.body.coordenadas
        })
    )
    }else{
        await producer.disconnect().then(
            res.status(200).json({
                error: "El carro no existe."
            })
        )
    }
});

app.listen(port, () => {
    console.log(`Listening on port ${port}`);
});