// microservice-patients/index.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
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

// ✅ RPC : CreatePatient
const createPatient = async (call, callback) => {
    try {
        const { nom, prenom, age, telephone } = call.request;
        const id = "pat_" + Date.now().toString(); 

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

// ✅ RPC : GetPatient (SQL corrigé ici 👇)
const getPatient = async (call, callback) => {
    try {
        // ❌ AVANT : 'SELECT * VALUES FROM patients WHERE id = ?'
        // ✅ APRÈS :
        const patient = await db.get(
            'SELECT * FROM patients WHERE id = ?', 
            [call.request.id]
        );
        
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
    db = await initDB();

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