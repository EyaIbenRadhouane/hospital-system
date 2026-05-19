// microservice-dmp/index.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PROTO_PATH = path.join(__dirname, '../protos/dmp.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
});
const dmpProto = grpc.loadPackageDefinition(packageDefinition).dmp;

// Base de données simulée en mémoire pour la démonstration
const dmpDatabase = {
    "pat_123": {
        id: "dmp_default",
        patient_id: "pat_123",
        diagnostic_principal: "Suivi post-opératoire standard",
        ordonnance: "Paracétamol 1g, 3 fois par jour pendant 5 jours",
        date_creation: new Date().toISOString()
    }
};

const getDmpByPatient = (call, callback) => {
    const patientId = call.request.patient_id;
    const dmpDoc = dmpDatabase[patientId];

    if (!dmpDoc) {
        // Retourne une ébauche par défaut pour que la démo GraphQL ne crash pas
        return callback(null, {
            id: "dmp_" + Date.now().toString(),
            patient_id: patientId,
            diagnostic_principal: "Ébauche de Dossier Médical Partagé",
            ordonnance: "Aucun traitement enregistré",
            date_creation: new Date().toISOString()
        });
    }
    callback(null, dmpDoc);
};

function main() {
    const server = new grpc.Server();
    server.addService(dmpProto.DmpService.service, {
        GetDmpByPatient: getDmpByPatient
    });

    const port = process.env.PORT || "50053";
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
        if (err) return console.error(err);
        console.log(`✓ MS DMP gRPC Server connecté sur le port ${boundPort}`);
    });
}

main();