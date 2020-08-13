const ns = 'kv-manager';
const kv = new pylon.KVNamespace(ns);
const dataPrefix = 'data';
const MAX_TAG_SIZE = 200;

type id_type = string;
type item_type = pylon.Json;

interface DataPtr extends pylon.JsonObject {
  tag: number;
  id: id_type;
}
type DataPtrCompress = string;
interface KVMHeader extends pylon.JsonObject {
  // lock: number;
  blocks: Array<number>;
  dataptr: {
    [k: string]: DataPtr;
  };
  next: number;
  nextID: number;
}
const newhdr: KVMHeader = {
  // lock: 1,
  blocks: [],
  dataptr: {},
  next: -1,
  nextID: 161 // First printable utf-16 code appears here, printable up to 7424
};

interface KVDataTag extends pylon.JsonObject {
  // lock: number;
  size: number;
  data: {
    [id: string]: item_type;
  };
}
const emptyTag: KVDataTag = {
  // lock: 1,
  size: 0,
  data: {}
};

function sizeof(obj: pylon.Json) {
  return new TextEncoder().encode(JSON.stringify(obj)).byteLength;
}

// This pair of functions
function num2str(n: number) {
  var ret: string = '';
  do {
    ret += String.fromCharCode(n % 65535);
    n = (n / 65535) >> 0;
  } while (n > 0);
  return ret;
}
function str2num(s: string) {
  var num: number = 0;
  for (var i = s.length - 1; i >= 0; i--) num = 65535 * num + s.charCodeAt(i);
  return num;
}

// KVManager manages the KV space for your bot, allowing you to store information more densely & bypass the 256 key limit.
//  - With 256 tags and 8kB per tag, we have 2MB space to play with.
//  - Future plan: Convert all items to bitstrings & then we can store more than 8kb in a key by splitting byte arrays.
//  - Current plan: Handle data storage > 2kb
//    - Support adding a chunk 'data1' when data0 fills up
//    - Support adding another header 'header1' when we have more than 8kb worth of keys (probs not happening)
//    - Request check-and-set with custom equality condition for faster setting
// Works by storing multiple `key` within each `tag` plus a few `header tag` to manage information & reduce search time.
class KVManager {
  protected static async getHeader(headerTag: string): Promise<KVMHeader> {
    const hdr = await kv.get<KVMHeader>(headerTag);
    if (hdr === undefined) {
      kv.put(headerTag, newhdr);
      return newhdr;
    }
    return hdr;
  }

  protected static async findKeyHeader(
    key: string
  ): Promise<[number, KVMHeader | undefined]> {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) return [-1, undefined];
    return [0, hdr];
  }
  protected static async findDataPtr(key: string): Promise<DataPtr> {
    const [hdrNum, hdr] = await KVManager.findKeyHeader(key);
    // const hdr = await KVManager.getHeader('header0');
    if (hdr == undefined) return { tag: -1, id: '' };

    return hdr.dataptr[key];
  }

  protected static async incrementID(): Promise<number> {
    const incremented = await kv.transactWithResult<KVMHeader, number>(
      'header0',
      (hdr = newhdr) => {
        return { next: { ...hdr, nextID: hdr.nextID + 1 }, result: hdr.nextID };
      }
    );
    return incremented.result;
  }

  // Creates/updates a header entry for a key
  protected static async updateKeyHdr(hdrTag: string, k: string, dp: DataPtr) {
    kv.transact<KVMHeader>(hdrTag, (hdr = newhdr) => {
      var ret = {
        ...hdr,
        dataptr: { ...hdr.dataptr, [k]: dp }
      };
      // if (str2num(dp.id) == hdr.nextID) ret.nextID++;
      return ret;
    });
  }

  // Deletes a key's header entry
  protected static async deleteKeyHdr(headerTag: string, key: string) {
    kv.transact<KVMHeader>(headerTag, (hdr) => {
      if (hdr == undefined) return;
      if (Object.keys(hdr.dataptr).length == 1) return;
      var ret = { ...hdr, dataptr: { ...hdr.dataptr } };
      delete ret.dataptr[key];
      return ret;
    });
  }

  // Adds blocks to header if needed. Only first header has a blocklist
  protected static async addBlock(dataBlock: number) {
    kv.transact<KVMHeader>('header0', (hdr = newhdr) => {
      if (!(dataBlock in hdr.blocks)) {
        return { ...hdr, blocks: [...hdr.blocks, dataBlock] };
      }
      return hdr;
    });
  }

  // Remove a block from the header.
  protected static async removeBlock(dataBlock: number) {
    kv.transact<KVMHeader>('header0', (hdr = newhdr) => {
      if (dataBlock in hdr.blocks) {
        var ret = { ...hdr, blocks: [...hdr.blocks] };
        ret.blocks.splice(ret.blocks.indexOf(dataBlock), 1);
        return ret;
      }
    });
  }

  // Updates a key within a tag.
  protected static async updateKeyTag(dptr: DataPtr, v: item_type) {
    kv.transact<KVDataTag>(dataPrefix + dptr.tag, (prev = emptyTag) => {
      var ret = { ...prev, data: { ...prev.data, [dptr.id]: v } };
      ret.size = sizeof(ret);
      return ret;
    });
  }

  // Deletes a key within a tag
  protected static async deleteKeyTag(dptr: DataPtr) {
    const tag = dataPrefix + dptr.tag;
    await kv.transact<KVDataTag>(tag, (prev) => {
      if (prev == undefined) return;
      if (Object.keys(prev.data).length == 1) return undefined;
      var ret = { ...prev, data: { ...prev.data } };
      delete ret.data[dptr.id];
      ret.size = sizeof(ret);
      return ret;
    });
    var datum = await kv.get<KVDataTag>(tag);
    if (datum == undefined) {
      KVManager.removeBlock(dptr.tag);
    }
  }

  protected static async getData(dptr: DataPtr) {
    const tag = dataPrefix + dptr.tag;
    const datum = await kv.get<KVDataTag>(tag);
    return datum ? datum.data[dptr.id] : undefined;
  }

  protected static async itemUpdateWillFit(
    dptr: DataPtr,
    newValue: item_type
  ): Promise<boolean> {
    const datum = await kv.get<KVDataTag>(dataPrefix + dptr.tag);
    if (datum == undefined) return false;
    const prevSize = sizeof({ [dptr.id]: datum.data[dptr.id] });
    const newSize = sizeof({ [dptr.id]: newValue });
    return datum.size - prevSize + newSize < MAX_TAG_SIZE;
  }

  // findEmptyTag finds a tag with enough space to fit a certain size.
  protected static async findEmptyTag(size: number): Promise<number> {
    const hdr = await KVManager.getHeader('header0'); // This one always header0
    var max: number = -1;

    var promises = [];
    for (var b of hdr.blocks) {
      const blockNum = b as number;
      max = blockNum > max ? (blockNum as number) : max;
      promises.push(
        kv.get<KVDataTag>(dataPrefix + blockNum).then((datum) => {
          if ((datum ? datum.size : 0) + size < MAX_TAG_SIZE) 
            throw blockNum;
        })
      );
    }
    return Promise.all(promises)
      .then(() => {
        // We had to add a new block.
        var tagNum = max + 1;
        if (!(tagNum in hdr.blocks)) KVManager.addBlock(tagNum);
        return tagNum;
      })
      .catch((blockNum) => {
        return blockNum;
      });
  }

  static async get(key: string) {
    const dptr = await KVManager.findDataPtr(key);
    return dptr.tag != -1 ? KVManager.getData(dptr) : undefined;
  }

  // Cases:
  //  Key doesn't exist yet
  //  Key already exists & enough space
  //  Key doesn't exist and needs to start a new tag
  //  Key already exists but needs to be moved to a new tag
  static async set(key: string, value: pylon.Json) {
    var dptr = await KVManager.findDataPtr(key);
    var findNewTag = true;

    // Check for in-place update case. Its simpler.
    if (dptr.tag != -1) {
      findNewTag = !(await KVManager.itemUpdateWillFit(dptr, value));
      // If it exists but needs to be moved, we delete it from its original tag.
      if (findNewTag) KVManager.deleteKeyTag(dptr);
    } else {
      // If key does not exist yet, assign new id
      dptr.id = num2str(await KVManager.incrementID());
    }

    // If the value needs a new home, search for it here.
    if (findNewTag) {
      const size = sizeof({ [dptr.id]: value });
      dptr.tag = await KVManager.findEmptyTag(size);
      KVManager.updateKeyHdr('header0', key, dptr);
    }

    KVManager.updateKeyTag(dptr, value); // Need to replace tagNum & key with dptr stuff.
  }

  static async delete(key: string) {
    const [hdrNum, hdr] = await KVManager.findKeyHeader(key);
    if (hdr == undefined) return;
    const dptr = hdr.dataptr[key];
    // const dptr = await KVManager.findDataPtr(key);

    KVManager.deleteKeyHdr('header' + hdrNum, key);
    KVManager.deleteKeyTag(dptr);
  }

  static async keysUsed() {
    return kv.count();
  }

  static async numKeys() {
    const hdr = await KVManager.getHeader('header0');
    return Object.keys(hdr.dataptr).length;
  }

  static async clear() {
    return kv.clear();
  }

  // get internal structure for debug
  static async items() {
    return kv.items();
  }

  static async getArrayBuffer(key: string) {
    return kv.getArrayBuffer(key);
  }
}

export default KVManager;
