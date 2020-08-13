# PylonKVManager
Writing a KV manager to get around the 256 key limit. It works by keeping a header tag or two to track where all the information is stored.

## Usage
No initialization needed. Just import `KVManager` and use it like this:

```typescript
import KVManager from './KVManager';

await KVManager.set('asdf', 20);
const v = await KVManager.get('asdf');  // v = 20
```

## TODO
- Support multiple headers for when that overflows > 8kb
  - Create new header when we're about to overflow
  - Search for keys in multiple headers
- Custom serialization for efficiency
- Implement unique id generator (counter?) & switch to number id (4byte id?)
