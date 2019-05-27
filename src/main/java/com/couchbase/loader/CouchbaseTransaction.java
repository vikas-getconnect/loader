package com.couchbase.loader;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.Transactions;
import com.couchbase.transactions.config.TransactionConfig;
import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CouchbaseTransaction {
    private static final String CHAR_LIST =
            "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int NTHREDS=8;
    private static TransactionHelper transactionHelper=new TransactionHelper();
    private static final Logger log = LogManager.getLogger(CouchbaseTransaction.class);


    public static void main(String[] args) throws InterruptedException {
        CouchbaseTransaction couchbaseTransaction=new CouchbaseTransaction();
        ReadArgs param= new ReadArgs();
        param.setArgs(args);

        TransactionConfig config = transactionHelper.createTransactionConfig(600, 0);

        Cluster cluster=Cluster.connect(ClusterEnvironment.create(param.getHostname(),param.getUsername(),param.getPassword()));
        Bucket bucket=cluster.bucket("default");
        Collection collection = bucket.defaultCollection();

        Transactions tr = transactionHelper.createTansaction(cluster,config);
        log.info("transaction created");
        //System.out.println("transaction created");
        while (true) {
            RateLimiter rateLimiter = RateLimiter.create(1);
            if (rateLimiter.tryAcquire()) {
                rateLimiter.acquire();
                ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
                for (int i = 0; i < param.getThreads(); i++) {
                    List<Tuple2<String, JsonObject>> documents = couchbaseTransaction.getDocumentsJson(param.getCreateCount());
                    RunTransactions worker = new RunTransactions(tr, collection, documents, param);
                    executor.execute(worker);
                }
                executor.shutdown();
                // Wait until all threads are finish
                executor.awaitTermination(600, TimeUnit.SECONDS);
                log.info("Finished all threads");
            }
        }
//        while (true){
//            final ScheduledThreadPoolExecutor service = new ScheduledThreadPoolExecutor(NTHREDS);
//            for (int i = 0; i < param.getThreads(); i++) {
//                List<Tuple2<String, JsonObject>> documents = couchbaseTransaction.getDocumentsJson(param.getCreateCount());
//                int delay = 1000 * service.getQueue().size() / param.getOps();
//                RunTransactions worker = new RunTransactions(tr, collection, documents,param);
//                service.schedule(worker,delay,TimeUnit.MILLISECONDS);
//            }
//            service.shutdown();
//            service.awaitTermination(60,TimeUnit.SECONDS);
//            System.out.println("Finished all threads");
//        }

    }


    public static List<String> generateKeys(Integer number){
        List<String> keys=new ArrayList<>();
        for (int i = 0; i < number; i++) {
            keys.add(generateRandomStringUsingSecureRandom(10));
        }
        return keys;
    }

    public  List<Tuple2<String, JsonObject>> getDocumentsJson(Integer number){
        List<String> keys=generateKeys(number);
        JsonObject json = JsonObject.fromJson(getJson());
        List<Tuple2<String,JsonObject>> doc=new ArrayList<>();
        for (String key:keys) {
            //key= key+"-"+ System.nanoTime();
            Tuple2<String, JsonObject> jdoc=Tuples.of(key,json);
            doc.add(jdoc);
        }
        return doc;
    }

    public static List<Tuple2<String, Object>> getDocuments(Integer number){
        List<String> keys=generateKeys(number);
        JsonObject json = JsonObject.create();
        json.put("vikas","test");
        List<Tuple2<String,Object>> doc=new ArrayList<>();
        for (String key:keys) {
            //key= key+"-"+ System.nanoTime();
            Tuple2<String, Object> jdoc=Tuples.of(key,json);
            doc.add(jdoc);
        }
        return doc;
    }

    public static String generateRandomStringUsingSecureRandom(int length) {
        StringBuffer randStr = new StringBuffer(length);
        SecureRandom secureRandom = new SecureRandom();
        for (int i = 0; i < length; i++)
            randStr.append(CHAR_LIST.charAt(secureRandom.nextInt(CHAR_LIST.length())));
        return randStr.toString();
    }

    public String getJson(){
        StringBuilder resultBuilder = new StringBuilder("");
        try {
            InputStream inputStream = getClass().getClassLoader().getResourceAsStream("template.json");
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                resultBuilder.append(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultBuilder.toString();
    }

}
