package com.hanji.search;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.auth.oauth2.GoogleCredentials;

import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Akash Eldo (axe1412)
 */
public class Database {

    private Firestore db;

    public Database(){
        // Use a service account
        try {
            InputStream serviceAccount = new FileInputStream("C:\\Users\\akash\\Projects\\Hanji Search\\hanjisearch\\hanji-bd63d-849ae0babd80.json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
            FirebaseOptions options = new FirebaseOptions.Builder()
                    .setCredentials(credentials)
                    .build();
            if(FirebaseApp.getApps().isEmpty()) { //<--- check with this line
                FirebaseApp.initializeApp(options);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        db = FirestoreClient.getFirestore();
    }

    public Map<String,Object> fetchDoc(String id) throws InterruptedException,ExecutionException {
        ApiFuture<DocumentSnapshot> query = this.db.collection("words").document(id).get();
        DocumentSnapshot documentSnapshot = query.get();
        Map<String,Object> map = documentSnapshot.getData();
        map.put("id",documentSnapshot.getId());
        return map;
    }

    public static void main(String[] args){
        Database database = new Database();

        // asynchronously retrieve all users
        try {
            System.out.println(database.fetchDoc("가하다1"));
        }catch (InterruptedException | ExecutionException e){
            e.printStackTrace();
        }
    }
}
