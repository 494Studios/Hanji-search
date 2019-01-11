package com.hanji.search;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.auth.oauth2.GoogleCredentials;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Akash Eldo (axe1412)
 */
public class Database {

    public static Firestore db = null;

    public Database() {
        if (db == null) {
            try {
                // Use the application default credentials
                GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
                FirebaseOptions options = new FirebaseOptions.Builder()
                        .setCredentials(credentials)
                        .setProjectId("hanji-bd63d")
                        .build();
                FirebaseApp.initializeApp(options);
                if (FirebaseApp.getApps().isEmpty()) { //<--- check with this line
                    FirebaseApp.initializeApp(options);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            db = FirestoreClient.getFirestore();
        }
    }

    public ArrayList<Map<String,Object>> getAllDocs(){
        ArrayList<Map<String,Object>> docs = new ArrayList<>();

        //asynchronously retrieve all documents that haven't been indexed already
        ApiFuture<QuerySnapshot> future = db.collection("words").whereEqualTo("indexed",false).get();
        try {
            // future.get() blocks on response
            List<QueryDocumentSnapshot> documents = future.get().getDocuments();
            for (QueryDocumentSnapshot document : documents) {
                System.out.println(new String(document.getId().getBytes(), Charset.forName("UTF-8")));

                Map<String,Object> map = document.getData();
                map.put("id",document.getId());
                docs.add(map);
                //document.getReference().update("indexed",true).get(); // this document has been indexed
            }
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
        return docs;
    }

    public void markDocAsIndexed(String id){
        //asynchronously retrieve all documents that haven't been indexed already
        ApiFuture<DocumentSnapshot> future = db.collection("words").document(id).get();
        try {
            // future.get() blocks on response
            DocumentSnapshot document = future.get();
            document.getReference().update("indexed",true).get(); // this document has been indexed
        }catch (ExecutionException | InterruptedException e){
            e.printStackTrace();
        }
    }


    public Map<String,Object> fetchDoc(String id) throws InterruptedException,ExecutionException {
        ApiFuture<DocumentSnapshot> query = Database.db.collection("words").document(id).get();
        DocumentSnapshot documentSnapshot = query.get();
        Map<String,Object> map = documentSnapshot.getData();
        map.put("id",documentSnapshot.getId());
        return map;
    }

    // Use to clear index for testing
    public static void main(String[] args){
        ApiFuture<QuerySnapshot> query = new Database().db.collection("words").get();
        try {
            List<QueryDocumentSnapshot> documents = query.get().getDocuments();
            for (QueryDocumentSnapshot document : documents) {
                document.getReference().update("indexed",false).get(); // this document has been indexed
                System.out.println(document.getId());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
