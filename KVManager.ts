const ns = 'kv-manager';
const kv = new pylon.KVNamespace(ns);
const dataPrefix = 'data';

interface TagPointer {
  [dat: string]: number;
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

class KVManager {
  protected static async getHeader(headerTag: string): Promise<KVMHeader> {
    const hdr = await kv.get<KVMHeader>(headerTag);
    if (hdr === undefined || hdr == null) {
      kv.put(headerTag, newhdr);
      return newhdr;
    }
    return hdr;
  }

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

  protected static async addKey(headerTag: string, key: string, tag: number) {
    kv.transact<KVMHeader>(headerTag, (hdr = newhdr) => {
      var ret = { ...hdr, dataptr: { ...hdr.dataptr } };
      ret.dataptr[key] = tag;
      return ret;
    });
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

  static async get(key: string) {
    // return kv.get(key);
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) {
      return undefined;
    }

    const tag = dataPrefix + hdr.dataptr[key];
    const dat = await kv.get<pylon.JsonObject>(tag);
    if (dat == undefined)
      throw new Error('Something in the structure fucked up');
    return dat[key];
  }

  static async delete(key: string) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) return;

    const tag = dataPrefix + hdr.dataptr[key];
    KVManager.deleteTag(tag, 'header0', key);
    // const dat = await kv.get<pylon.JsonObject>(tag);
  }

  static async keysUsed() {
    return kv.count();
  }

  static async numKeys() {
    return kv.count();
  }

  static async clear() {
    return kv.clear();
  }

  static async items() {
    return kv.items();
  }
}

export default KVManager;
