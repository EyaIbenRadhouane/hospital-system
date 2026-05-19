// microservice-dmp/index.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { Kafka } = require('kafkajs');
const { initDMPDB } = require('./db');

const PROTO_PATH = path.join(__dirname, '../protos/dmp.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});

const dmpProto = grpc.loadPackageDefinition(packageDefinition).dmp;

// Configuration Kafka
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'dmp-service', brokers: [kafkaBroker] });
const consumer = kafka.consumer({ groupId: 'dmp-group' });

let rxDb;

// RPC : Lire le DMP d'un patient
const getDmpByPatient = async (call, callback) => {
    try {
        const patientId = call.request.patient_id;
        // Requête RxDB NoSQL
        const dmpDoc = await rxDb.dmps.findOne({
            selector: { patient_id: patientId }
        }).exec();

        if (!dmpDoc) {
            return callback({
                code: grpc.status.NOT_FOUND,
                details: "Aucun Dossier Médical Partagé trouvé pour ce patient."
            });
        }

        callback(null, {
            id: dmpDoc.id,
            patient_id: dmpDoc.patient_id,
            diagnostic_principal: dmpDoc.diagnostic_principal,
            ordonnance: dmpDoc.ordonnance,
            date_creation: dmpDoc.date_creation
        });
    } catch (err) {
        callback({ code: grpc.status.INTERNAL, details: err.message });
    }
};

// Fonction pour écouter le Topic Kafka (Asynchrone)
async function startKafkaConsumer() {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: 'rendezvous-events', fromBeginning: true }); [cite: 231]

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const rawValue = message.value.toString();
                const eventData = JSON.parse(rawValue);

                console.log(`➔ Message Kafka intercepté sur le topic [${topic}]:`, eventData);

                // Si l'événement est bien la clôture du RDV
                if (eventData.event === 'RendezVousTermine') { [cite: 231]
                    const newId = "dmp_" + Date.now().toString();
                    
                    // Insertion automatique dans la base NoSQL RxDB
                    await rxDb.dmps.insert({
                        id: newId,
                        patient_id: eventData.patient_id,
                        diagnostic_principal: "Ébauche automatique (Consultation Clôturée)", [cite: 232]
                        ordonnance: "À renseigner par le médecin",
                        date_creation: new Date().toISOString()
                    });

                    console.log(`✓ RxDB: Ébauche de DMP créée avec succès pour le patient ${eventData.patient_id}`); [cite: 232]
                }
            },
        });
    } catch (e) {
        console.error("Erreur de communication Kafka Consommateur:", e.message);
    }
}

async function main() {
    // 1. Initialiser RxDB
    rxDb = await initDMPDB();

    // 2. Démarrer le Consommateur Kafka en tâche de fond
    startKafkaConsumer();

    // 3. Démarrer le serveur gRPC
    const server = new grpc.Server();
    server.addService(dmpProto.DmpService.service, {
        GetDmpByPatient: getDmpByPatient
    });

    const port = process.env.PORT || "50053"; [cite: 227]
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
        if (err) return console.error(err);
        console.log(`✓ MS DMP gRPC Server connecté sur le port ${boundPort}`);
    });
}

main();