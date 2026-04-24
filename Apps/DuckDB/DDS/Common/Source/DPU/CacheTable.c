#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "CacheTable.h"
#include "HashFunctions.h"

//
// Initialize the cache table
//
//
int
InitCacheTable(
    CacheTableT** CacheTable
) {
    *CacheTable = malloc(sizeof(CacheTableT*));
    if (!(*CacheTable)) {
        return -1;
    }
    
    (*CacheTable)->Table = (CacheBucketT**)(malloc(sizeof(CacheBucketT*) * CACHE_TABLE_BUCKET_COUNT));
    if (!(*CacheTable)->Table) {
        return -1;
    }

    for (size_t i = 0; i != CACHE_TABLE_BUCKET_COUNT; i++) {
        (*CacheTable)->Table[i] = (CacheBucketT*)aligned_alloc(DDS_CACHE_LINE_SIZE, sizeof(CacheBucketT));
        if (!(*CacheTable)->Table[i]) {
            return -1;
        }
        memset((*CacheTable)->Table[i], 0, sizeof(CacheBucketT));
    }

    return 0;
}

//
// Add an item to the cache table
//
//
int
AddToCacheTable(
    CacheTableT* CacheTable,
    CacheItemT* Item
) {
    size_t maxDepth = (size_t) CACHE_TABLE_BUCKET_COUNT_POWER << 2;
    if (maxDepth > CACHE_TABLE_CAPACITY) {
        maxDepth = CACHE_TABLE_CAPACITY;
    }

    uint32_t mask = CACHE_TABLE_BUCKET_COUNT - 1;
    uint32_t offset = 0;
    CacheElementT ele1, ele2;
    CacheElementT *carrier = &ele1, *victim = &ele2;
    memcpy(&carrier->Item, Item, sizeof(CacheItemT));
    
    HASH_FUNCTION1(&Item->Key, sizeof(KeyT), carrier->Hash1);
    HASH_FUNCTION2(&Item->Key, sizeof(KeyT), carrier->Hash2);
    if (carrier->Hash1 == carrier->Hash2) {
        carrier->Hash2 = ~carrier->Hash1;
    }

    uint32_t bucketPos;
    HashValueT *hashVector;
    CacheBucketT *targetBucket;
    CacheElementT *targetElement;
    CacheElementT *tmp;
    HashValueT tmpV;
    
    for (size_t depth = 0; depth < maxDepth; ++depth) {
        bucketPos = carrier->Hash1 & mask;
        targetBucket = CacheTable->Table[bucketPos];

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
        targetBucket->Occ = 1;
#endif

        hashVector = targetBucket->HashValues;
        for (int e = 0; e != CACHE_TABLE_BUCKET_SIZE; e++) {
            if (!hashVector[e]) {
                //
                // This slot is available
                //
                //
                targetElement = &CacheTable->Table[bucketPos]->Elements[e];
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                targetElement->Occ = 1;
#endif
                memcpy(targetElement, carrier, sizeof(CacheElementT));
                hashVector[e] = carrier->Hash1;
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                targetElement->Occ = 0;
#endif

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
                targetBucket->Occ = 0;
#endif
                return 0;
            }
            else if (hashVector[e] == carrier->Hash1) {
                //
                // Check key
                //
                //
                targetElement = &CacheTable->Table[bucketPos]->Elements[e];
                if (targetElement->Item.Key == carrier->Item.Key) {
                    //
                    // Key already exists
                    // Update the value
                    //
                    //
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                    targetElement->Occ = 1;
#endif
                    memcpy(&targetElement->Item, &carrier->Item, sizeof(CacheItemT));
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                    targetElement->Occ = 0;
#endif

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
                    targetBucket->Occ = 0;
#endif
                    return 0;
                }
            }
        }

        //
        // This bucket is full, so pick one element as the victim
        //
        //
        targetElement = &targetBucket->Elements[offset];
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
        targetElement->Occ = 1;
#endif
        memcpy(victim, targetElement, sizeof(CacheElementT));
        memcpy(targetElement, carrier, sizeof(CacheElementT));
        hashVector[offset] = carrier->Hash1;
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
        targetElement->Occ = 0;
#endif
        tmpV = victim->Hash1;
        victim->Hash1 = victim->Hash2;
        victim->Hash2 = tmpV;

        //
        // Victim becomes the carrier
        //
        //
        tmp = carrier;
        carrier = victim;
        victim = tmp;

        if (++offset == CACHE_TABLE_BUCKET_SIZE) {
            offset = 0;
        }

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
        targetBucket->Occ = 0;
#endif
    }

    //
    // Slot not found
    // Undo the insertion
    //
    //
    for (size_t depth = 0; depth < maxDepth; ++depth) {
        bucketPos = carrier->Hash2 & mask;

        if (offset-- == 0) {
            offset = CACHE_TABLE_BUCKET_SIZE - 1;
        }

        targetBucket = CacheTable->Table[bucketPos];
        hashVector = targetBucket->HashValues;
        targetElement = &targetBucket->Elements[offset];

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
        targetBucket->Occ = 1;
#endif

        //
        // Swap the elements
        //
        //
        tmpV = carrier->Hash1;
        carrier->Hash1 = carrier->Hash2;
        carrier->Hash2 = tmpV;
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
        targetElement->Occ = 1;
#endif
        memcpy(victim, targetElement, sizeof(CacheElementT));
        memcpy(targetElement, carrier, sizeof(CacheElementT));
        hashVector[offset] = carrier->Hash2;
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
        targetElement->Occ = 0;
#endif

        //
        // Victim becomes the carrier
        //
        //
        tmp = carrier;
        carrier = victim;
        victim = tmp;

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
        targetBucket->Occ = 0;
#endif
    }

    assert(memcmp(&carrier->Item, Item, sizeof(CacheItemT)) == 0);

    return -1;
}

//
// Delete an item from the cache table
//
//
void
DeleteFromCacheTable(
    CacheTableT* CacheTable,
    KeyT* Key
) {
    uint32_t mask = CACHE_TABLE_BUCKET_COUNT - 1;

    HashValueT *hashVector;
    CacheBucketT *targetBucket;
    CacheElementT *targetElement;
    HashValueT hash1, hash2;
    HASH_FUNCTION1(Key, sizeof(KeyT), hash1);

    //
    // Check the first hash function
    //
    //
    targetBucket = CacheTable->Table[hash1 & mask];
    hashVector = targetBucket->HashValues;

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
    targetBucket->Occ = 1;
#endif

    for (int e = 0; e != CACHE_TABLE_BUCKET_SIZE; e++) {
        if (hashVector[e] == hash1) {
            targetElement = &targetBucket->Elements[e];
            if (targetElement->Item.Key == *Key) {
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                targetElement->Occ = 1;
#endif
                memset(targetElement, 0, sizeof(CacheElementT));
                hashVector[e] = 0;
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                targetElement->Occ = 0;
#endif

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
                targetBucket->Occ = 0;
#endif
                return;
            }
        }
    }

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
    targetBucket->Occ = 0;
#endif

    //
    // Check the second hash function
    //
    //
    HASH_FUNCTION2(Key, sizeof(KeyT), hash2);
    if (hash1 == hash2) {
        hash2 = ~hash1;
    }
    targetBucket = CacheTable->Table[hash2 & mask];
    hashVector = targetBucket->HashValues;

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
    targetBucket->Occ = 1;
#endif

    for (int e = 0; e != CACHE_TABLE_BUCKET_SIZE; e++) {
        if (hashVector[e] == hash2) {
            targetElement = &targetBucket->Elements[e];
            if (targetElement->Item.Key == *Key) {
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                targetElement->Occ = 1;
#endif
                memset(targetElement, 0, sizeof(CacheElementT));
                hashVector[e] = 0;
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
                targetElement->Occ = 0;
#endif

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
                targetBucket->Occ = 1;
#endif
                return;
            }
        }
    }

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
    targetBucket->Occ = 0;
#endif
}

//
// Look up the cache table given a key
//
//
CacheItemT*
LookUpCacheTable(
    CacheTableT* CacheTable,
    KeyT* Key
) {
    const uint32_t mask = CACHE_TABLE_BUCKET_COUNT - 1;
    const KeyT key = *Key;

    HashValueT *hashVector;
    CacheBucketT *targetBucket;
    CacheElementT *targetElement;
    HashValueT hash1, hash2;

    //
    // Check the first hash function
    //
    //
    HASH_FUNCTION1(Key, sizeof(KeyT), hash1);
    targetBucket = CacheTable->Table[hash1 & mask];

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
    if (targetBucket->Occ) {
        goto LookupSecondHash;
    }
#endif

    hashVector = targetBucket->HashValues;
    
    for (int e = 0; e != CACHE_TABLE_BUCKET_SIZE; e++) {
        if (hash1 == hashVector[e]) {
            targetElement = &targetBucket->Elements[e];
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
            if (targetElement->Occ) {
                continue;
            }
#endif
            if (targetElement->Item.Key == key) {
                return &targetElement->Item;
            }
        }
    }

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
LookupSecondHash:
#endif
    //
    // Check the second hash function
    //
    //
    HASH_FUNCTION2(Key, sizeof(KeyT), hash2);
    if (hash1 == hash2) {
        hash2 = ~hash1;
    }

    targetBucket = CacheTable->Table[hash2 & mask];

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
    if (targetBucket->Occ) {
        goto LookupFail;
    }
#endif

    hashVector = targetBucket->HashValues;
    
    for (int e = 0; e != CACHE_TABLE_BUCKET_SIZE; e++) {
        if (hash2 == hashVector[e]) {
            targetElement = &targetBucket->Elements[e];
#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_ITEM
            if (targetElement->Occ) {
                continue;
            }
#endif
            if (targetElement->Item.Key == key) {
                return &targetElement->Item;
            }
        }
    }

#if CACHE_TABLE_OCC_GRANULARITY == CACHE_TABLE_OCC_GRANULARITY_BUCKET
LookupFail:
#endif
    return (CacheItemT*)NULL;
}

//
// Destroy the cache table
//
//
void
DestroyCacheTable(
    CacheTableT* CacheTable
) {
    if (CacheTable && CacheTable->Table) {
        for (size_t i = 0; i != CACHE_TABLE_BUCKET_COUNT; i++) {
            if (CacheTable->Table[i]) {
                free(CacheTable->Table[i]);
            }
        }

        free(CacheTable->Table);
    }
}