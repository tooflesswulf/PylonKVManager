const ns = 'kv-manager';
const kv = new pylon.KVNamespace(ns);
const dataPrefix = 'data';
const MAX_TAG_SIZE = 100;

type id_type = string;
type item_type = pylon.Json;

interface DataPtr extends pylon.JsonObject {
  tag: number;
  id: id_type;
}
interface KVMHeader extends pylon.JsonObject {
  // lock: number;
  blocks: pylon.JsonArray;
  dataptr: {
    [k: string]: DataPtr;
  };
  next: number;
}
const newhdr: KVMHeader = {
  // lock: 1,
  blocks: [],
  dataptr: {},
  next: -1
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

  // Creates/updates a header entry for a key
  protected static async updateKeyHdr(hdrTag: string, k: string, dp: DataPtr) {
    kv.transact<KVMHeader>(hdrTag, (hdr = newhdr) => {
      var ret = {
        ...hdr,
        dataptr: { ...hdr.dataptr, [k]: dp }
      };
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

  protected static async findTagSpace(
    k: id_type,
    v: item_type
  ): Promise<number> {
    const size = sizeof({ [k]: v });
    const hdr = await KVManager.getHeader('header0');
    var max: number = -1;
    for (var b of hdr.blocks) {
      const blockNum = b as number;
      max = blockNum > max ? (blockNum as number) : max;
      const datum = await kv.get<KVDataTag>(dataPrefix + blockNum);
      if ((datum ? datum.size : 0) + size < MAX_TAG_SIZE) {
        return blockNum;
      }
    }
    var tagNum = max + 1;
    if (!(tagNum in hdr.blocks)) KVManager.addBlock(tagNum);
    return tagNum;
  }

  static async get(key: string) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) return undefined;

    const dptr = hdr.dataptr[key];
    return KVManager.getData(dptr);
  }

  // Cases:
  //  Key doesn't exist yet
  //  Key already exists & enough space
  //  Key doesn't exist and needs to start a new tag
  //  Key already exists but needs to be moved to a new tag
  static async set(key: string, value: pylon.Json) {
    const hdr = await KVManager.getHeader('header0');

    var dptr: DataPtr = { id: key, tag: -1 };
    var findNewTag = false;

    // Check for in-place update case. Its simpler.
    if (key in hdr.dataptr) {
      dptr = hdr.dataptr[key];
      findNewTag = await KVManager.itemUpdateWillFit(dptr, value);
      if (!findNewTag) {
        // If it exists but needs to be moved, we delete it from its corresponding tag.
        KVManager.deleteKeyTag(dptr);
      }
    }

    if (!findNewTag) {
      dptr.tag = await KVManager.findTagSpace(dptr.id, value);
      KVManager.updateKeyHdr('header0', key, dptr);
    }

    KVManager.updateKeyTag(dptr, value); // Need to replace tagNum & key with dptr stuff.
  }

  static async delete(key: string) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) return;
    const dptr = hdr.dataptr[key];

    KVManager.deleteKeyHdr('header0', key);
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
