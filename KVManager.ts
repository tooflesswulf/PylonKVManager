const ns = 'kv-manager';
const kv = new pylon.KVNamespace(ns);
const dataPrefix = 'data';
const MAX_TAG_SIZE = 100;

interface TagPointer {
  [key: string]: number;
}

interface KVMHeader extends pylon.JsonObject {
  // lock: number;
  blocks: pylon.JsonArray;
  dataptr: TagPointer;
}
const newhdr: KVMHeader = {
  // lock: 1,
  blocks: [],
  dataptr: {}
};

interface KVDataTag extends pylon.JsonObject {
  // lock: number;
  size: number;
  data: pylon.JsonObject;
}
const emptyTag: KVDataTag = {
  // lock: 1,
  size: 0,
  data: {}
};

function sizeof(obj: pylon.Json) {
  const str = JSON.stringify(obj);
  var m = encodeURIComponent(str).match(/%[89ABab]/g);

  return str.length + (m ? m.length : 0);
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
  protected static async updateKeyHdr(hdrTag: string, k: string, tag: number) {
    kv.transact<KVMHeader>(hdrTag, (hdr = newhdr) => {
      var ret = { ...hdr, dataptr: { ...hdr.dataptr, [k]: tag } };
      return ret;
    });
  }

  // Deletes a key's header entry
  protected static async deleteKeyHdr(headerTag: string, key: string) {
    kv.transact<KVMHeader>(headerTag, (hdr) => {
      if (hdr == undefined) return;
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
  protected static async updateKeyTag(tag: string, k: string, v: pylon.Json) {
    kv.transact<KVDataTag>(tag, (prev = emptyTag) => {
      var ret = { ...prev, data: { ...prev.data, [k]: v } };
      ret.size = sizeof(ret);
      return ret;
    });
  }

  // Deletes a key within a tag
  protected static async deleteKeyTag(tag: string, key: string) {
    await kv.transact<KVDataTag>(tag, (prev) => {
      if (prev == undefined) return;
      var ret = { ...prev, data: { ...prev.data } };
      delete ret.data[key];
      ret.size = sizeof(ret);
      return ret;
    });
    var datum = await kv.get<KVDataTag>(tag);
    if (datum ? datum.data.length == 0 : false) {
      kv.delete(tag);
      KVManager.removeBlock(1); // Get number for realsies later
    }
  }

  static async get(key: string) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) return undefined;

    const datum = await kv.get<KVDataTag>(dataPrefix + hdr.dataptr[key]);
    if (datum == undefined)
      throw new Error('Something fucked up. Recommend you to clear it all');
    return datum.data[key];
  }

  // Cases:
  //  Key doesn't exist yet
  //  Key already exists & enough space
  //  Key doesn't exist and needs to start a new tag
  //  Key already exists but needs to be moved to a new tag
  static async set(key: string, value: pylon.Json) {
    const hdr = await KVManager.getHeader('header0');
    var tagNum = hdr.blocks.length;

    const addSize = sizeof({ [key]: value });

    // Get tag number to insert into
    for (var b of hdr.blocks) {
      const datum = await kv.get<KVDataTag>(dataPrefix + b);
      if ((datum ? datum.size : 0) + addSize < MAX_TAG_SIZE) {
        tagNum = b as number;
        break;
      }
    }
    // Check for in-place update case. Its simpler.
    if (key in hdr.dataptr) {
      const datum = await kv.get<KVDataTag>(dataPrefix + hdr.dataptr[key]);
      if (datum == undefined)
        throw new Error('Something fucked up. Recommend you to clear it all');
      var item = datum.data[key];

      if (datum.size - sizeof({ [key]: item }) + addSize < MAX_TAG_SIZE) {
        tagNum = hdr.dataptr[key];
        // We should really not do the for loop above, but im lazy
      } else {
        // If it exists but needs to be moved, we do delete it from its corresponding tag.
        KVManager.deleteKeyTag(dataPrefix + hdr.dataptr[key], key);
        KVManager.updateKeyHdr('header0', key, tagNum);
      }
    }

    if (!(key in hdr.dataptr)) {
      KVManager.updateKeyHdr('header0', key, tagNum);
    }
    if (!(tagNum in hdr.blocks)) KVManager.addBlock(tagNum);
    KVManager.updateKeyTag(dataPrefix + tagNum, key, value);
  }

  static async delete(key: string) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) return;

    KVManager.deleteKeyHdr('header0', key);
    KVManager.deleteKeyTag(dataPrefix + hdr.dataptr[key], key);
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
}

export default KVManager;
