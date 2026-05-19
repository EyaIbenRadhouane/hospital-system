// api-gateway/index.js
const express = require('express');
const { ApolloServer, gql } = require('apollo-server-express');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const app = express();
app.use(express.json());

// ---------------------------------------------------------------------------
// 1. CONFIGURATION ET CHARGEMENT DES CLIENTS gRPC
// ---------------------------------------------------------------------------
const loadProto = (protoName) => {
    const protoPath = path.join(__dirname, '../protos', `${protoName}.proto`);
    const packageDef = protoLoader.loadSync(protoPath, {
        keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
    });
    return grpc.loadPackageDefinition(packageDef)[protoName];
};

// URLs des services (utilisent les noms réseau Docker ou localhost par défaut)
const PATIENTS_URL = process.env.PATIENTS_SERVICE_URL || 'localhost:50051';
const RENDEZVOUS_URL = process.env.RENDEZVOUS_SERVICE_URL || 'localhost:50052';
const DMP_URL = process.env.DMP_SERVICE_URL || 'localhost:50053';

// Instanciation des clients gRPC
const patientsProto = loadProto('patients');
const patientsClient = new patientsProto.PatientsService(PATIENTS_URL, grpc.credentials.createInsecure());

const rdvProto = loadProto('rendezvous');
const rdvClient = new rdvProto.RendezVousService(RENDEZVOUS_URL, grpc.credentials.createInsecure());

const dmpProto = loadProto('dmp');
const dmpClient = new dmpProto.DmpService(DMP_URL, grpc.credentials.createInsecure());


// ---------------------------------------------------------------------------
// 2. EXPOSITION DES ENDPOINTS REST (Validation Postman Étapes 1, 2, 3)
// ---------------------------------------------------------------------------

// Étape 1 : Création Administrative du Patient (POST /patients) 
app.post('/patients', (req, res) => {
    patientsClient.CreatePatient(req.body, (err, response) => {
        if (err) return res.status(500).json({ error: err.details });
        res.status(201).json(response); // 201 Created 
    });
});

// Étape 2 : Planification du Rendez-vous (POST /rendezvous) 
app.post('/rendezvous', (req, res) => {
    rdvClient.CreateRendezVous(req.body, (err, response) => {
        if (err) return res.status(500).json({ error: err.details });
        res.status(201).json(response); // 201 Created 
    });
});

// Étape 3 : Clôture du RDV & Déclenchement Kafka (PUT /rendezvous/:id) 
app.put('/rendezvous/:id', (req, res) => {
    const payload = { id: req.params.id, statut: "Termine" }; // Force le statut "Termine" pour le scénario Kafka [cite: 231, 237]
    rdvClient.UpdateRendezVousStatus(payload, (err, response) => {
        if (err) return res.status(500).json({ error: err.details });
        res.status(200).json(response); // 200 OK 
    });
});


// ---------------------------------------------------------------------------
// 3. EXPOSITION DE GRAPHQL : FÉDÉRATION DASHBOARD MÉDECIN (Étape 4)
// ---------------------------------------------------------------------------

// Schéma GraphQL unifié [cite: 82, 130]
const typeDefs = gql`
    type Patient {
        id: String!
        nom: String!
        prenom: String!
        age: Int!
        telephone: String
    }

    type DMP {
        id: String!
        patient_id: String!
        diagnostic_principal: String!
        ordonnance: String!
        date_creation: String!
    }

    type DashboardMedecin {
        patient: Patient
        dmp: DMP
    }

    type Query {
        getDashboardMedecin(patient_id: String!): DashboardMedecin
    }
`;

// Resolvers réalisant la fédération de données unifiée (SQLite3 + RxDB) via gRPC [cite: 216, 237]
const resolvers = {
    Query: {
        getDashboardMedecin: async (_, { patient_id }) => {
            // Promisification des appels gRPC pour paralléliser les requêtes
            const getPatient = () => new Promise((resolve) => {
                patientsClient.GetPatient({ id: patient_id }, (err, res) => resolve(err ? null : res));
            });

            const getDmp = () => new Promise((resolve) => {
                dmpClient.GetDmpByPatient({ patient_id }, (err, res) => resolve(err ? null : res));
            });

            // Récupération simultanée des données administratives et médicales 
            const [patientData, dmpData] = await Promise.all([getPatient(), getDmp()]);

            return {
                patient: patientData,
                dmp: dmpData
            };
        }
    }
};

// Initialisation et démarrage d'Apollo Server sur l'API Gateway
async function startServer() {
    const server = new ApolloServer({ typeDefs, resolvers });
    await server.start();
    server.applyMiddleware({ app, path: '/graphql' });

    const restPort = process.env.REST_PORT || 3000;
    app.listen(restPort, () => {
        console.log(`✓ API Gateway REST connectée sur http://localhost:${restPort}`);
        console.log(`✓ API Gateway GraphQL connectée sur http://localhost:${restPort}/graphql`);
    });
}

startServer();