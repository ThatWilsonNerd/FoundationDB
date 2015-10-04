#define FDB_API_VERSION 20

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "foundationdb/fdb_c.h"

void checkError(fdb_error_t errorNum)
{
    if(errorNum) { fprintf(stderr, "Error (%d): %s\n", errorNum, fdb_get_error(errorNum)); exit(errorNum); }
}

void waitAndCheckError(FDBFuture *future)
{
    checkError(fdb_future_block_until_ready(future));
    if(fdb_future_is_error(future))
    {
        checkError(fdb_future_get_error(future, NULL));
    }
}

void runNetwork()
{
    checkError(fdb_run_network());
}


void createData(FDBDatabase *db)
{
    int committed = 0;
    //  create transaction
    FDBTransaction *tr;
    checkError(fdb_database_create_transaction(db, &tr));
    while(!committed)
    {
        //  create data
        char *key1 = "Test Key1";
        char *val1 = "Test Value1";
        char *key2 = "Test Key2";
        char *val2 = "Test Value2";
        char *key3 = "Test Key3";
        char *val3 = "Test Value3";
        fdb_transaction_set(tr, key1, (int)strlen(key1), val1, (int)strlen(val1));
        fdb_transaction_set(tr, key2, (int)strlen(key2), val2, (int)strlen(val2));
        fdb_transaction_set(tr, key3, (int)strlen(key3), val3, (int)strlen(val3));

        //  commit to database
        FDBFuture *commitFuture = fdb_transaction_commit(tr);
        checkError(fdb_future_block_until_ready(commitFuture));
        if(fdb_future_is_error(commitFuture)) {
            waitAndCheckError(fdb_transaction_on_error(tr, fdb_future_get_error(commitFuture, NULL)));
        }
        else
        {
            committed = 1;
        }
        fdb_future_destroy(commitFuture);
    }
    //  destroy transaction
    fdb_transaction_destroy(tr);
}

void readData(FDBDatabase *db)
{
    //  get value
    FDBTransaction *tr;
    checkError(fdb_database_create_transaction(db, &tr));
    char *key = "Test Key2";
    FDBFuture *getFuture = fdb_transaction_get(tr, key, (int)strlen(key), 0);
    waitAndCheckError(getFuture);
    
    fdb_bool_t valuePresent;
    const uint8_t *value;
    int valueLength;
    checkError(fdb_future_get_value(getFuture, &valuePresent, &value, &valueLength));

    printf("Got Value for %s: '%.*s'\n", key, valueLength, value);
    fdb_transaction_destroy(tr);
    fdb_future_destroy(getFuture);
}

void readAllData(FDBDatabase *db)
{
    FDBTransaction *tr;
    checkError(fdb_database_create_transaction(db, &tr));
    
    //  read range
    char *begin_key_name = "";
    fdb_bool_t begin_or_equal = 0;
    int begin_offset = 0;
    char *end_key_name = "";
    fdb_bool_t end_or_equal = 0;
    int end_offset = 10;
    int limit = 0;
    int target_bytes = 0;
    int iteration = 0;
    FDBFuture *getFuture = fdb_transaction_get_range(tr, begin_key_name, (int)strlen(begin_key_name), begin_or_equal, begin_offset, end_key_name, (int)strlen(end_key_name), end_or_equal, end_offset, limit, target_bytes, FDB_STREAMING_MODE_WANT_ALL, iteration, 0, 0);
    waitAndCheckError(getFuture);
    
    FDBKeyValue** out_kv;
    int* out_count;
    fdb_bool_t* out_more;
    checkError(fdb_future_get_keyvalue_array(getFuture, &out_kv, &out_count, &out_more));
    printf("Key/Value Pairs Returned: %i\n",(int)out_count);
    
    FDBKeyValue *kv = out_kv;
    for(int i=0; i<(int)out_count;i++)
    {
        printf("%.*s: '%.*s'\n",kv->key_length,kv->key,kv->value_length,kv->value);
        kv++;
    }
    fdb_future_destroy(getFuture);
    fdb_transaction_destroy(tr);
}

int main(int argc, char **argv)
{
    puts("Starting FoundationDB C API Test");
    if(argc < 2)
    {
        //  use default cluster file if no/Users/willne specified
        argv[1] ="/usr/local/etc/foundationdb/fdb.cluster";
    }

    //  set up network
    checkError(fdb_select_api_version(FDB_API_VERSION));
    checkError(fdb_setup_network());
    puts("Got network");

    // run network
    pthread_t netThread;
    pthread_create(&netThread, NULL, (void *)runNetwork, NULL);

    //  get cluster
    FDBFuture *clusterFuture = fdb_create_cluster(argv[1]);
    waitAndCheckError(clusterFuture);
    puts("Got cluster");

    //  get database
    FDBCluster *cluster;
    checkError(fdb_future_get_cluster(clusterFuture, &cluster));
    fdb_future_destroy(clusterFuture);
    FDBFuture *dbFuture = fdb_cluster_create_database(cluster, "DB", 2);
    waitAndCheckError(dbFuture);
    FDBDatabase *db;
    checkError(fdb_future_get_database(dbFuture, &db));
    fdb_future_destroy(dbFuture);
    puts("Got database");
    
    //  create some data
    //createData(db);
    
    //  read data
    readData(db);
    
    //  read all key/value pairs
    //readAllData(db);
    
    //  shutdown
    checkError(fdb_stop_network());
    fdb_database_destroy(db);
    fdb_cluster_destroy(cluster);
    puts("Program done. Now exiting...");
    pthread_exit(NULL);
    return 0;
 }
