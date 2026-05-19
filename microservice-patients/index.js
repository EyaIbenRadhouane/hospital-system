// microservice-patients/index.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { v4: uuidv4 } = require('uuid'); // Si non installé, utilise Date.now().toString() pour aller vite
const { initDB } = require('./db');

const PROTO_PATH = path.join(__dirname, '../protos/patients.proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});

const patientsProto = grpc.loadPackageDefinition(packageDefinition).patients;

let db;

// Implémentation des RPC du contrat .proto
const createPatient = async (call, callback) => {
    try {
        const { nom, prenom, age, telephone } = call.request;
        const id = Date.now().toString(); // ID unique rapide sans dépendance externe stricte
        
        await db.run(
            'INSERT INTO patients (id, nom, prenom, age, telephone) VALUES (?, ?, ?, ?, ?)',
            [id, nom, prenom, age, telephone]
        );
        
        callback(null, { id, nom, prenom, age, telephone });
    } catch (err) {
        callback({
            code: grpc.status.INTERNAL,
            details: "Erreur lors de l'insertion du patient : " + err.message
        });
    }
};

const getPatient = async (call, callback) => {
    try {
        const patient = await db.get('SELECT * FROM patients WHERE id = ?', [call.request.id]);
        if (!patient) {
            return callback({
                code: grpc.status.NOT_FOUND,
                details: "Patient non trouvé"
            });
        }
        callback(null, patient);
    } catch (err) {
        callback({
            code: grpc.status.INTERNAL,
            details: err.message
        });
    }
};

async function main() {
    db = await initDB(); // Initier SQLite3

    const server = new grpc.Server();
    server.addService(patientsProto.PatientsService.service, {
        CreatePatient: createPatient,
        GetPatient: getPatient
    });

    const port = process.env.PORT || "50051";
    server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
        if (err) {
            console.error(err);
            return;
        }
        console.log(`✓ MS Patients gRPC Server connecté sur le port ${boundPort}`);
    });
}

main();