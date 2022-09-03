#ifndef LRUCACHE_H
#define LRUCACHE_H

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>

typedef struct PageId
{
    u_int64_t DbId;
    u_int64_t TSpcId;
    u_int64_t RelId;

    u_int64_t BlockNum;
} PageId;

extern void LruCacheInit();

extern bool PutKey(PageId key, u_int64_t lsn);

extern bool GetValueWithKey(PageId key, u_int64_t lsn);

#endif