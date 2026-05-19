// microservice-rendezvous/index.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { Kafka } = require('kafkajs');

const PROTO_PATH = path.join(__dirname, '../protos/rendezvous.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});

const rendezvousProto = grpc.loadPackageDefinition(packageDefinition).rendezvous;

// Config Kafka Client
const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';
const kafka = new Kafka({ clientId: 'rendezvous-service', brokers: [kafkaBroker] });
const producer = kafka.producer();

// Base de données locale temporaire en mémoire pour les RDV
const rendezvousStorage = {};

const createRendezVous = (call, callback) => {
    const { patient_id, medecin_id, date_heure } = call.request;
    const id = "rdv_" + Date.now().toString();
    
    const newRDV = { id, patient_id, medecin_id, date_heure, statut: "Confirme" };
    rendezvousStorage[id] = newRDV;

    callback(null, newRDV);
};

const updateRendezVousStatus = async (call, callback) => {
    const { id, statut } = call.request;
    
    if (!rendezvousStorage[id]) {
        return callback({ code: grpc.status.NOT_FOUND, details: "Rendez-vous introuvable" });
    }

    rendezvousStorage[id].statut = statut;
    const updatedRDV = rendezvousStorage[id];

    // SCÉNARIO CRITIQUE KAFKA [cite: 228, 229]
    // Si le statut passe à "Termine", on émet l'événement pour le DMP [cite: 231, 232]
    if (statut === "Termine") {
        try {
            await producer.send({
                topic: 'rendezvous-events',
                messages: [
                    { 
                        key: updatedRDV.patient_id, 
                        value: JSON.stringify({
                            event: 'RendezVousTermine',
                            patient_id: updatedRDV.patient_id,
                            medecin_id: updatedRDV.medecin_id
                        }) 
                    }
                ],
            });
            console.log(`➔ Événement Kafka produit: Rendez-vous ${id} Terminé.`);
        } catch (kafkaErr) {
            console.error("Échec d'envoi du message Kafka:", kafkaErr);
        }
    }

    callback(null, updatedRDV);
};

async function main() {
    // Connexion au broker Kafka avant de lancer le serveur gRPC
    try {
        await producer.connect();
        console.log('✓ Connecté au Broker Kafka avec succès (Producteur).');
    } catch (e) {
        console.error('Impossible de se connecter à Kafka, démarrage gRPC seul...', e.message);
    }

    const server = new grpc.Server();
    server.addService(rendezvousProto.RendezVousService.service, {
        CreateRendezVous: createRendezVous,
        UpdateRendezVousStatus: updateRendezVousStatus
    });

    const port = process.env.PORT || "50052";
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
        if (err) return console.error(err);
        console.log(`✓ MS Rendez-vous gRPC Server connecté sur le port ${boundPort}`);
    });
}

main();