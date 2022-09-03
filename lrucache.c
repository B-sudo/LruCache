#include "lrucache.h"

#define HashArraySize 100   //TODO is 100 enough?

typedef struct MultiVersionPageData
{
    struct MultiVersionPageData * next;

    PageId key;
    u_int64_t lsn;
    //TODO record PageData in PageHandler or just point it to Data?
    char * value;
} MultiVersionPageData;


typedef struct PageHandler
{
    struct PageHandler * hash_prev;
    struct PageHandler * hash_next;
    struct PageHandler * link_prev;
    struct PageHandler * link_next;

    PageId key;
    //TODO how to GC? truncate with LSN?
    MultiVersionPageData * head;
} PageHandler;

typedef struct LruCacheMeta
{
    int key_num;
    PageHandler ** hash_array;
    PageHandler * link_head;    //recent use
    PageHandler * link_tail;    // least recent use
} LruCacheMeta;

static LruCacheMeta * lrucache_meta;

int doHash(PageId key)      //TODO we need a new way to do hash
{
    return (key.DbId & key.TSpcId & key.RelId & key.BlockNum) % HashArraySize;
}

void LruCacheInit()
{
    //TODO we should use ShmemInit. malloc->palloc free->pfree. 
    //TODO How to track the use of memory? Maybe lrucache_meta->key_num will help.
    lrucache_meta = (LruCacheMeta *)malloc(sizeof(LruCacheMeta));
    lrucache_meta->key_num = 0;
    lrucache_meta->hash_array = (PageHandler **)malloc(sizeof(PageHandler *) * HashArraySize);
    lrucache_meta->link_head = NULL;
    lrucache_meta->link_tail = NULL;
}

MultiVersionPageData * InitMVPage(PageId key, u_int64_t lsn)
{
    MultiVersionPageData * mvpage;

    mvpage = (MultiVersionPageData *)malloc(sizeof(MultiVersionPageData));

    mvpage->next = NULL;
    mvpage->value = NULL;   //TODO value should be a BLCKSZ page

    mvpage->key = key;
    mvpage->lsn = lsn;

    return mvpage;
}

void FreeMVPage(MultiVersionPageData * mvpage)
{
    //Shortcut for truncate
    //TODO remember to free the actual page block
    assert(mvpage != NULL);
    //TODO assert -> Assert?
    free(mvpage);
}

PageHandler * InitHandlerWithKey(PageId key)
{
    PageHandler * handler;

    handler = (PageHandler *)malloc(sizeof(PageHandler));

    handler->hash_next = NULL;
    handler->hash_prev = NULL;
    handler->link_next = NULL;
    handler->link_prev = NULL;
    handler->head = NULL;

    handler->key = key;

    return handler;
}

void FreeHandler(PageHandler * handler)
{
    assert(handler != NULL);
    assert(handler->hash_next == NULL);
    assert(handler->link_next == NULL);
    assert(handler->head == NULL);

    free(handler);
}

void InsertHandlerToHT(PageHandler * handler)
{
    int hashindex;
    PageHandler * hook;

    assert(handler != NULL);
    assert(lrucache_meta != NULL);
    assert(lrucache_meta->hash_array != NULL);

    hashindex = doHash(handler->key);

    if (lrucache_meta->hash_array[hashindex] == NULL)
        lrucache_meta->hash_array[hashindex] = handler;
    else 
    {
        hook = lrucache_meta->hash_array[hashindex];
        lrucache_meta->hash_array[hashindex] = handler;
        handler->hash_next = hook;
        hook->hash_prev = handler;
    }
}

void DeleteHandlerFromHT(int hashindex, PageHandler * handler)
{
    assert(handler != NULL);
    assert(handler->head == NULL);

    if (handler->hash_prev == NULL) //the first one in hash chain
    {
        lrucache_meta->hash_array[hashindex] = handler->hash_next;
        if (handler->hash_next != NULL)
            handler->hash_next->hash_prev = NULL;
    }
    else
    {
        handler->hash_prev->hash_next = handler->hash_next;
        if (handler->hash_next != NULL)
            handler->hash_next->hash_prev = handler->hash_prev;
    }
    //No need to free handler
    handler->hash_next = NULL;
    handler->hash_prev = NULL;
}

PageHandler * GetHandlerFromHT(PageId key)
{
    int hashindex;
    PageHandler * result;

    assert(lrucache_meta != NULL);
    assert(lrucache_meta->hash_array != NULL);

    hashindex = doHash(key);

    if (lrucache_meta->hash_array[hashindex] == NULL)
        return NULL;
    else 
    {
        result = lrucache_meta->hash_array[hashindex];
        do
        {
            //TODO if renamed the PageId , we should change here
            if (result->key.TSpcId == key.TSpcId && result->key.DbId == key.DbId
            && result->key.RelId == key.RelId && result->key.BlockNum == key.BlockNum)
                return result;
            result = result->hash_next;
        } while (result != NULL);
        return result;
    }
}

void InsertHandlerToLink(PageHandler * handler)
{
    PageHandler * hook;
    //recent use, insert to head
    assert(handler != NULL);
    assert(lrucache_meta != NULL);

    if (lrucache_meta->link_head == NULL)
    {
        assert(lrucache_meta->link_tail == NULL);
        lrucache_meta->link_head = handler;
        lrucache_meta->link_tail = handler;
    }
    else
    {
        hook = lrucache_meta->link_head;
        lrucache_meta->link_head = handler;
        handler->link_next = hook;
        hook->link_prev = handler;
    }
}

void DeleteHandlerFromLink(PageHandler * handler)
{
    PageHandler * hook_prev;
    PageHandler * hook_next;

    assert(handler != NULL);

    hook_prev = handler->link_prev;
    hook_next = handler->link_next;

    if (hook_prev == NULL)  //first in head
    {
        if (hook_next == NULL)
        {
            lrucache_meta->link_head = NULL;
            lrucache_meta->link_tail = NULL;
        }
        else
        {
            lrucache_meta->link_head = hook_next;
            hook_next->link_prev = NULL;
        }
    }
    else 
    {
        if (hook_next == NULL)
        {
            lrucache_meta->link_tail = hook_prev;
            hook_prev->link_next = NULL;
        }
        else
        {
            hook_next->link_prev = hook_prev;
            hook_prev->link_next = hook_next;
        }
    }

    handler->link_next = NULL;
    handler->link_prev = NULL;
}

void UpdateHandlerInLink(PageHandler * handler)
{
    DeleteHandlerFromLink(handler);
    InsertHandlerToLink(handler);
}

bool InsertMVPageToHandler(PageHandler * handler, MultiVersionPageData * mvpage)
{   //return false for the same lsn
    MultiVersionPageData * hook;
    //LSN decreasing
    assert(handler != NULL);
    assert(mvpage != NULL);

    if (handler->head == NULL)
        handler->head = mvpage;
    else if (handler->head->lsn < mvpage->lsn)
    {
        mvpage->next = handler->head;
        handler->head = mvpage;
    }
    else
    {
        hook = handler->head;
        if (hook->lsn == mvpage->lsn)
            return false;
        while (hook->next != NULL && hook->next->lsn > mvpage->lsn)
            hook = hook->next;
        if (hook->next == NULL)
            hook->next = mvpage;
        else
        {
            if (hook->next->lsn == mvpage->lsn)
                return false;
            mvpage->next = hook->next;
            hook->next = mvpage;
        }
    }
    lrucache_meta->key_num++;
    return true;
}

void TruncateMVPageWithLSN(PageHandler * handler, u_int64_t lsn)
{
    MultiVersionPageData * hook;
    MultiVersionPageData * del_hook;
    int hashindex;
    //truncate all mvpages whose lsn are smaller than lsn
    assert(handler != NULL);
    
    if (handler->head == NULL)
        return ;
    else if (handler->head->lsn < lsn)
    {
        hook = handler->head;
        handler->head = NULL;
        do
        {
            del_hook = hook;
            hook = hook->next;
            FreeMVPage(del_hook);
            lrucache_meta->key_num--;
        } while(hook != NULL);
    }
    else
    {
        hook = handler->head;
        while(hook->next != NULL && hook->next->lsn >= lsn)
            hook = hook->next;
        if (hook->next == NULL)
            return ;
        else
        {
            hook = hook->next;
            do
            {
                del_hook = hook;
                hook = hook->next;
                FreeMVPage(del_hook);
                lrucache_meta->key_num--;
            } while(hook != NULL);
        }
    }

    if (handler->head == NULL)
    {
        hashindex = doHash(handler->key);
        DeleteHandlerFromHT(hashindex, handler);
        DeleteHandlerFromLink(handler);
    }

    FreeHandler(handler);
}

void TruncateAllMVPage(PageHandler * handler)
{
    MultiVersionPageData * hook;
    MultiVersionPageData * del_hook;
    int hashindex;

    assert(handler != NULL);
    assert(handler->head != NULL);

    hook = handler->head;
    handler->head = NULL;
    do
    {
        del_hook = hook;
        hook = hook->next;
        FreeMVPage(del_hook);
        lrucache_meta->key_num--;
    } while(hook != NULL);

    hashindex = doHash(handler->key);
    DeleteHandlerFromHT(hashindex, handler);
    DeleteHandlerFromLink(handler);
    FreeHandler(handler);
}

MultiVersionPageData * GetMVPageWithLSN(PageHandler * handler, u_int64_t lsn) 
{
    MultiVersionPageData * hook;
    hook = handler->head;
    while (hook != NULL)
    {
        if (hook->lsn > lsn)
            hook = hook->next;
        else if (hook->lsn == lsn)
            return hook;
        else
            return NULL;
    }
    return NULL;
}

//TODO static pthread_mutex_t putkey_mutex = PTHREAD_MUTEX_INITIALIZER;

bool PutKey(PageId key, u_int64_t lsn)  //TODO add value
{   //true for put key successfully, false for (key, lsn) already exists
    PageHandler * handler;
    MultiVersionPageData * mvpage;
    bool result;

    assert(lrucache_meta != NULL);

    mvpage = InitMVPage(key, lsn);

    handler = GetHandlerFromHT(key);

    if (handler == NULL)
    {
        handler = InitHandlerWithKey(key);
        //TODO pthread_mutex_lock(putkey_mutex);
        InsertHandlerToHT(handler);
        InsertHandlerToLink(handler);
        //TODO pthread_mutex_unlock(putkey_mutex);
    }

    //TODO pthread_mutex_lock(putkey_mutex);
    result = InsertMVPageToHandler(handler, mvpage);
    //TODO pthread_mutex_unlock(putkey_mutex);
    
    if (result)
        return true;

    FreeMVPage(mvpage);
    return false;
}

bool GetValueWithKey(PageId key, u_int64_t lsn) //TODO actually get value, not bool
{
    PageHandler * handler;
    MultiVersionPageData * mvpage;

    assert(lrucache_meta != NULL);

    handler = GetHandlerFromHT(key);

    if (handler == NULL)
        return false;
    else
    {
        mvpage = GetMVPageWithLSN(handler, lsn);
        if (mvpage == NULL)
            return false;
        else
        {
            UpdateHandlerInLink(handler);
            return true;
        }
    }
}

//TODO GC