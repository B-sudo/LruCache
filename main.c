#include "lrucache.h"

int main()
{
    PageId key1, key2, key3;

    key1.DbId = key1.TSpcId = key1.RelId = key1.BlockNum = 2;
    key2.DbId = key2.TSpcId = key2.RelId = key2.BlockNum = 5;
    key3.DbId = key3.TSpcId = key3.RelId = key3.BlockNum = 10;

    LruCacheInit();

    assert(PutKey(key1, 100));
    assert(PutKey(key2, 200));
    assert(PutKey(key3, 300));
    assert(GetValueWithKey(key1, 100));
    assert(GetValueWithKey(key2, 200));
    assert(!GetValueWithKey(key3, 100));
}
