const ns = 'kv-manager';
const kv = new pylon.KVNamespace(ns);
const dataPrefix = 'data';

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
    if (hdr === undefined || hdr == null) {
      kv.put(headerTag, newhdr);
      return newhdr;
    }
    return hdr;
  }

  // Updates a key within a tag.
  protected static async updateTag(
    tag: string,
    key: string,
    value: pylon.Json
  ) {
    kv.transact<pylon.JsonObject>(tag, (prev = {}) => {
      var ret = { ...prev };
      ret[key] = value;
      return ret;
    });
  }

  // Deletes a key within a tag & its corresponding header entry
  protected static async deleteTag(
    tag: string,
    headerTag: string,
    key: string
  ) {
    kv.transact<pylon.JsonObject>(tag, (prev = {}) => {
      var ret = { ...prev };
      delete ret[key];
      return ret;
    });
    kv.transact<KVMHeader>(headerTag, (hdr = newhdr) => {
      var ret = { ...hdr, dataptr: { ...hdr.dataptr } };
      delete ret.dataptr[key];
      return ret;
    });
  }

  // Creates a header entry for a new key
  protected static async addKey(headerTag: string, key: string, tag: number) {
    kv.transact<KVMHeader>(headerTag, (hdr = newhdr) => {
      var ret = { ...hdr, dataptr: { ...hdr.dataptr } };
      ret.dataptr[key] = tag;
      return ret;
    });
  }

  static async get(key: string) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) {
      return undefined;
    }

    const tag = dataPrefix + hdr.dataptr[key];
    const dat = await kv.get<pylon.JsonObject>(tag);
    if (dat == undefined)
      throw new Error('Something fucked up. Recommend you to clear it all');
    return dat[key];
  }

  static async set(key: string, value: pylon.Json) {
    const hdr = await KVManager.getHeader('header0');
    var tagNum = 0;
    if (!(key in hdr.dataptr)) {
      KVManager.addKey('header0', key, tagNum);
      console.log(`added key ${key}`);
    } else {
      tagNum = hdr.dataptr[key];
    }
    KVManager.updateTag(dataPrefix + tagNum, key, value);
  }

  static async delete(key: string) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) return;

    const tag = dataPrefix + hdr.dataptr[key];
    KVManager.deleteTag(tag, 'header0', key);
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
