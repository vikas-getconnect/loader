package com.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.transactions.*;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.LogDefer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class TransactionHelper {
    Queue<String> queue=new LinkedList<>();

    public TransactionConfig createTransactionConfig(int expiryTimeout, int durabilitylevel) {
        TransactionConfigBuilder config = TransactionConfigBuilder.create();
        switch(durabilitylevel) {
            case 0:
                config.durabilityLevel(TransactionDurabilityLevel.NONE);
                break;
            case 1:
                config.durabilityLevel(TransactionDurabilityLevel.MAJORITY);
                break;
            case 2:
                config.durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER);
                break;
            case 3:
                config.durabilityLevel(TransactionDurabilityLevel.PERSIST_TO_MAJORITY);
                break;
            default:
                config.durabilityLevel(TransactionDurabilityLevel.NONE);
        }

        return config.expirationTime(Duration.of((long)expiryTimeout, ChronoUnit.SECONDS)).build();
    }

    public Transactions createTansaction(Cluster cluster, TransactionConfig config) {
        return Transactions.create(cluster, config);
    }


    public Queue<String> multiInsertSingelTransaction(Transactions transaction, Collection collection, List<Tuple2<String, JsonObject>> documents) {
        final Queue<String>[] res = new Queue[]{new LinkedList<>()};
        try {
            transaction.reactive((ctx) -> {
                Map<String, Object> m = insertMulti(ctx, transaction, collection, documents);
                res[0]= (Queue<String>) m.get("ids");
                return (Mono<Void>) m.get("insert");
            }).block();
        } catch (TransactionFailed e) {
            System.out.println("Transaction " + e.result().transactionId() + " failed:");
            System.out.println("Error: " + e.result());
            for (LogDefer err : e.result().log().logs()) {
                if (err != null)
                    System.out.println(err.toString());
            }
            e.printStackTrace();
        }
        //System.out.println("queue:"+ res[0]);
        return res[0];
    }

    public Map<String, Object> insertMulti(AttemptContextReactive acr, Transactions transaction, Collection collection, List<Tuple2<String, JsonObject>> documents){
        Map<String,Object> map=new HashMap<>();
        Queue<String> ids=new LinkedList<>();
        ReactiveCollection reactiveCollection=collection.reactive();
        String id1 = documents.get(0).getT1();
        JsonObject content1 = documents.get(0).getT2();
        List<Tuple2<String, JsonObject>> remainingDocs = documents.stream().skip(1).collect(Collectors.toList());
        Mono<Void> multiInsert = acr.insert(reactiveCollection, id1, content1).map(i -> ids.add(i.id()))
                .flatMapMany(v -> Flux.fromIterable(remainingDocs)
                        .flatMap(doc ->
                                        acr.insert(reactiveCollection, doc.getT1() + System.nanoTime(), doc.getT2())
                                , remainingDocs.size()
                        ).map(t -> ids.add(t.id()))
                ).then();
        map.put("insert",multiInsert);
        map.put("ids",ids);
        return map;
    }

    public void multiUpdateSingelTransaction(Transactions transaction, Collection collection, List<String> ids) {
        try {
            TransactionResult result = transaction.reactive((ctx) -> {
                return updateMulti(ctx, transaction, collection, ids);
            }).block();
            //System.out.println("result: "+result.log().logs());
        } catch (TransactionFailed e) {
            System.out.println("Transaction " + e.result().transactionId() + " failed:");
            System.out.println("Error: " + e.result());
            for (LogDefer err : e.result().log().logs()) {
                if (err != null)
                    System.out.println(err.toString());
            }
        }
    }

    public Mono<Void> updateMulti(AttemptContextReactive acr, Transactions transaction, Collection collection, List<String> ids){
        ReactiveCollection reactiveCollection=collection.reactive();
        List<String> docToUpdate=ids.parallelStream().collect(Collectors.toList());
        String id1 = docToUpdate.get(0);
        List<String> remainingDocs = docToUpdate.stream().skip(1).collect(Collectors.toList());
        JsonObject json = JsonObject.create();
        json.put("vikas","update1");
        //System.out.println("docs to be updated"+docToUpdate);
        return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.replace(doc,json)).flatMapMany(
                v-> Flux.fromIterable(remainingDocs).flatMap(d -> acr.getOrError(reactiveCollection,d).flatMap(d1-> acr.replace(d1,json)),
                        remainingDocs.size())).then();
    }

    public void multiDeleteSingelTransaction(Transactions transaction, Collection collection, List<String> ids) {
        try {
            TransactionResult result = transaction.reactive((ctx) -> {
                return deleteMulti(ctx, transaction, collection, ids);
            }).block();
//            System.out.println("result: ");
//            for (LogDefer err : result.log().logs()) {
//                if (err != null)
//                    System.out.println(err.toString());
//            }
        } catch (TransactionFailed e) {
            System.out.println("Transaction " + e.result().transactionId() + " failed:");
            System.out.println("Error: " + e.result());
            for (LogDefer err : e.result().log().logs()) {
                if (err != null)
                    System.out.println(err.toString());
            }
        }
    }

    public Mono<Void> deleteMulti(AttemptContextReactive acr, Transactions transaction, Collection collection, List<String> ids){
        ReactiveCollection reactiveCollection=collection.reactive();
        List<String> docToDelete=ids.parallelStream().collect(Collectors.toList());
        String id1 = docToDelete.get(0);
        List<String> remainingDocs = docToDelete.stream().skip(1).collect(Collectors.toList());
        //System.out.println("docs to be deleted"+docToDelete);
        return acr.getOrError(reactiveCollection, id1).flatMap(doc-> acr.remove(doc)).thenMany(
                Flux.fromIterable(remainingDocs).flatMap(d -> acr.getOrError(reactiveCollection,d).flatMap(d1-> acr.remove(d1)),
                        remainingDocs.size())).then();
    }

    public void batchInsertSync(Transactions transaction, Collection collection, List<Tuple2<String, JsonObject>> documents){
        transaction.run((ctx) -> {
            for (Tuple2<String, JsonObject> t:documents) {
                final String id = t.getT1();
                final JsonObject content = t.getT2();
                ctx.insert(collection,id,content);
            }
            ctx.commit();
        });
    }

    public List<String> getQueue(int n){
        return this.queue.stream().skip(queue.size() - n).collect(Collectors.toList());
    }
}