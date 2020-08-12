const ns = 'kv-manager';
const kv = new pylon.KVNamespace(ns);

interface KVMHeader extends pylon.JsonObject {
  // lock: number;
  blocks: pylon.JsonArray;
  dataptr: pylon.JsonObject;
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

  protected static async addKey(headerTag: string, key: string, tag: string) {
    kv.transact<KVMHeader>(headerTag, (hdr = newhdr) => {
      var ret = { ...hdr, dataptr: { ...hdr.dataptr } };
      ret.dataptr[key] = tag;
      return ret;
    });
  }

  static async set(key: string, value: pylon.Json) {
    const hdr = await KVManager.getHeader('header0');
    if (!(key in hdr.dataptr)) {
      KVManager.addKey('header0', key, 'data0');
      console.log(`added key ${key}`);
    }
    KVManager.updateTag('data0', key, value);
  }

  static async get(key: string) {
    // return kv.get(key);
    const hdr = await KVManager.getHeader('header0');
    return JSON.stringify(hdr);
  }

  static async delete(key: string) {
    kv.delete(key);
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
