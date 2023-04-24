import firebase from "firebase";
import csv from "csvtojson";
import { firebaseConfig } from "./firebase-config.js";

import dotenv from "dotenv";
dotenv.config();

// Initialize Cloud Firestore through Firebase
firebase.initializeApp(firebaseConfig);

var db = firebase.firestore();

export async function getCollectionSize(collectionName) {
  const collectionRef = db.collection(collectionName);

  return collectionRef.get().then((querySnapshot) => {
    const count = querySnapshot.size;
    return count;
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

async function getDataFromCsv(fileName) {
  return csv()
    .fromFile(`./csv-input/${fileName}.csv`)
    .then((data) => data);
}

async function batchUploadCollection(fileName, collectionName, size = 500) {
  const data = await getDataFromCsv(fileName);
  console.log(`csv size is ${data?.length}`);
  return new Promise((resolve) => {
    uploadDataBatch({
      db,
      data,
      size,
      startPoint: 0,
      collectionName,
      resolve,
    });
  });
}

async function uploadDataBatch({
  db,
  data,
  size,
  startPoint,
  collectionName,
  resolve,
}) {
  const collectionRef = db.collection(collectionName);
  const start = startPoint;
  const end = startPoint + size;
  const batchData = data.slice(start, end);

  if (batchData.length === 0) {
    resolve("all uploaded finish");
    return;
  }

  const restData = data.slice(end, data.length);

  const batch = db.batch();

  batchData.forEach((doc) => {
    const docRef = collectionRef.doc(doc.client_id);
    batch.set(docRef, {
      client_id: doc.client_id,
      test_label: doc.event_param.pi_score,
    });
  });

  await batch.commit();

  console.log(`uploaded ${batchData.length}`);

  // Recurse on the next process tick, to avoid
  // exploding the stackuploadDataBatch.
  process.nextTick(() => {
    uploadDataBatch({
      db,
      data: restData,
      size,
      startPoint,
      collectionName,
      resolve,
    });
  });
}

async function deleteCollection(collectionPath) {
  const collectionRef = db.collection(collectionPath);
  const batchSize = 500;
  const query = collectionRef.orderBy("__name__").limit(batchSize);

  return new Promise((resolve) => {
    deleteQueryBatch(db, query, resolve);
  });
}

async function deleteQueryBatch(db, query, resolve) {
  const snapshot = await query.get();

  const batchSize = snapshot.size;
  if (batchSize === 0) {
    // When there are no documents left, we are done
    resolve();
    return;
  }

  // Delete documents in a batch
  const batch = db.batch();
  snapshot.docs.forEach((doc) => {
    batch.delete(doc.ref);
  });
  await batch.commit();
  console.log(`deleted ${batchSize}`);

  // Recurse on the next process tick, to avoid
  // exploding the stack.
  process.nextTick(() => {
    deleteQueryBatch(db, query, resolve);
  });
}

// get collection size
// signinUser().then(() => {
//   getCollectionSize("test-gaIds");
// });

// delete and upload collection
signinUser().then(async () => {
  const collectionName = "test";
  const fileName = "testFile";

  await deleteCollection(collectionName);
  await batchUploadCollection(fileName, collectionName)
    .then(() => console.log("All uploaded done"))
    .catch((err) => console.log(err));
  await getCollectionSize("test").then((collectionSize) => {
    console.log(`collection length is ${collectionSize}`);
  });
});

// delete collection
// signinUser().then(() => {
//   deleteCollection("test")
//     .then(() => console.log("All deleted done"))
//     .catch((err) => console.log(err));
// });
