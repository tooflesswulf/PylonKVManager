# PylonKVManager
Writing a KV manager to get around the 256 key limit. It works by keeping a header tag or two to track where all the information is stored.

## Usage
No initialization needed. Just import `KVManager` and use it like this:

```typescript
import KVManager from './KVManager';

await KVManager.set('asdf', 20);
const v = await KVManager.get('asdf');  // v = 20
```

Here's the list of functions:

`get` `set` `delete`
`tagsUsed` `listKeys` `numKeys` `clear`

(I hope the functions are more or less self-explanatory)

## How it works
Terminology- I call the original kv keys `tags` and the user stored keys `keys`. Also I use an intermediary 1-character unique identifier `id`. 

### Example
Here's the resulting internal structure after inserting 5 items, with an artificial limit of 100 characters per tag.
```typescript
KVManager.set('dataitem1', 'data1 data1 data1');
KVManager.set('dataitem2', 'data2 data2 data2');
KVManager.set('dataitem3', 'data3 data3 data3');
KVManager.set('a', 'data4 data4 data4');
KVManager.set('b', 'data5 data5 data5');
```

```
data0:      {"size":95,"data":{"¡":"data1 data1 data1","¢":"data2 data2 data2","£":"data3 data3 data3"}}
data1:      {"size":70,"data":{"¤":"data4 data4 data4","¥":"data5 data5 data5"}}
header0:    {"blocks":[0,1],"dataptr":{"dataitem1":"¡¡","dataitem2":"¡¢"},"nextID":166}
header1:    {"blocks":[],"dataptr":{"dataitem3":"¡£","a":"¢¤","b":"¢¥"},"nextID":161}
```
The tags are: data0, data1, header0, header1

The keys are: dataitem1, dataitem2, dataitem3, a, b

The ids are:  ¡, ¢, £, ¤, ¥




## TODO
- Optional items in json?
- Custom serialization for efficiency
- Implement splitting of data > 8kb into multiple tags (maybe?)
- Ask for customizable check-and-set function, and add a lock bit or write-counter so we don't have to do 8kb of comparison in `kv.transact()`
