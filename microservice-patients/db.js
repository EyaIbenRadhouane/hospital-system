// microservice-patients/db.js
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbPath = path.join(__dirname, 'patients.db');

const initDB = () => {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(dbPath, (err) => {
            if (err) {
                console.error('❌ Erreur connexion DB :', err.message);
                return reject(err);
            }
            console.log('✓ Connecté à patients.db');
        });

        // Table conforme à vos champs : nom, prenom, age, telephone
        db.run(`
            CREATE TABLE IF NOT EXISTS patients (
                id TEXT PRIMARY KEY,
                nom TEXT NOT NULL,
                prenom TEXT NOT NULL,
                age INTEGER,
                telephone TEXT
            )
        `, (err) => {
            if (err) {
                console.error('❌ Erreur création table :', err.message);
                return reject(err);
            }
            console.log('✓ Table "patients" initialisée');
            resolve(db);
        });
    });
};

module.exports = { initDB };