import firebase from "firebase";
import csv from "csvtojson";
import { firebaseConfig } from "./firebase-config.js";

import dotenv from "dotenv";
dotenv.config();

// Initialize Cloud Firestore through Firebase
firebase.initializeApp(firebaseConfig);

var db = firebase.firestore();

export function getCollectionSize(collectionName) {
  const collectionRef = db.collection(collectionName);

  collectionRef
    .get()
    .then((querySnapshot) => {
      const count = querySnapshot.size;
      console.log(`Number of documents in the collection: ${count}`);
    })
    .catch((error) => {
      console.log(`Error getting document count: ${error}`);
    });
}

export function signinUser() {
  return firebase
    .auth()
    .signInWithEmailAndPassword(
      process.env.accountName,
      process.env.accountPassword
    )
    .catch((error) => {
      console.log("Something went wrong with sign up: ", error);
    });
}

async function batchUploadCollection(fileName, collectionName) {
  csv()
    .fromFile(`./csv-input/${fileName}.csv`)
    .then(async (data) => {
      const collectionRef = db.collection(collectionName);

      const batches = [];
      const batchSize = 400;

      for (let i = 0; i < data.length; i += batchSize) {
        const batch = db.batch();
        const batchData = data.slice(i, i + batchSize);

        batchData.forEach((doc) => {
          const docRef = collectionRef.doc(doc.client_id);
          batch.set(docRef, {
            client_id: doc.client_id,
            test_label: doc.test_label,
          });
        });

        batches.push(batch);
      }

      console.log(
        `Uploading ${batches.length} batches of ${batchSize} documents`
      );

      for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        await new Promise((resolve) => {
          setTimeout(() => {
            batch.commit().then(() => {
              console.log(`updated the batch: ${i}`);
              resolve();
            });
          }, 1000);
        });
      }
      console.log("Batch upload complete");
    })
    .catch((err) => console.log(err));
}

// get collection size
// signinUser().then(() => {
//   getCollectionSize("test");
// });

// update collection
signinUser().then(() => {
  batchUploadCollection("test", "test");
});
