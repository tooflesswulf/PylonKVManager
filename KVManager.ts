const ns = 'kv-manager';
const kv = new pylon.KVNamespace(ns);
const headerPrefix = 'header';
const dataPrefix = 'data';
const MAX_TAG_SIZE = 2000; // Give it a solid chunk of wiggle room b/c size estimation is inaccurate.

type id_type = string;
type item_type = pylon.Json;

interface DataPtr extends pylon.JsonObject {
  tag: number;
  id: id_type;
}
type DP2 = string; // Compressed data pointer.
interface KVMHeader extends pylon.JsonObject {
  // lock: number;
  blocks: Array<number>;
  dataptr: {
    [k: string]: DP2;
    // [k: string]: DataPtr;
  };
  nextID: number;
}
const newhdr: KVMHeader = {
  // lock: 1,
  blocks: [],
  dataptr: {},
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

var headerCache: { [key: string]: KVMHeader } = {};
var dataCache: { [key: string]: KVDataTag } = {};

function sizeof(obj: pylon.Json) {
  return new TextEncoder().encode(JSON.stringify(obj)).byteLength;
}

// This pair of functions encodes a number in base 65535 as a string. (only really useful for >100)
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
  for (let i = s.length - 1; i >= 0; i--) num = 65535 * num + s.charCodeAt(i);
  return num;
}

const compressDataPtr = (dptr: DataPtr): DP2 =>
  num2str(dptr.tag + 161) + dptr.id;
const expandDP2 = (dp2: DP2): DataPtr => {
  return { tag: str2num(dp2[0]) - 161, id: dp2.substr(1) };
};

const hdrNum = (hdrTag: string) => parseInt(hdrTag.substr(headerPrefix.length));
const dataNum = (datTag: string) => parseInt(datTag.substr(dataPrefix.length));
async function NotPromiseAny<T, V>(
  list: T[],
  mapFunction: (arg0: T) => Promise<V | undefined>,
  allFailed: () => Promise<V>
): Promise<V> {
  for (const v of list) {
    const out = await mapFunction(v);
    if (out !== undefined) return out;
  }
  return allFailed();
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
  protected static async getHeader(
    headerTag: string,
    cache?: boolean
  ): Promise<KVMHeader> {
    if (cache) {
      if (headerTag in headerCache) return headerCache[headerTag];
    }
    const hdr = await kv.get<KVMHeader>(headerTag);
    if (hdr === undefined) {
      kv.put(headerTag, newhdr);
      return newhdr;
    }
    headerCache[headerTag] = hdr;
    return hdr;
  }

  // Crazy ass ~~asynchronous~~ search. Looks through all headers for a key.
  // Now is synchronous. Turns out async is faster but consumes my compute time like crazy
  //   Return: That header's tag plus the header object, or ['', undefined] if key is not found.
  protected static async findKeyHeader(
    key: string,
    cache?: boolean
  ): Promise<[string, KVMHeader | undefined]> {
    const headers = (await kv.list({ from: headerPrefix })).filter((tag) =>
      tag.startsWith(headerPrefix)
    );
    return NotPromiseAny<string, [string, KVMHeader | undefined]>(
      headers,
      async (hdrTag: string) => {
        const hdr = await KVManager.getHeader(hdrTag, cache);
        if (key in hdr.dataptr) return [hdrTag, hdr];
      },
      async () => ['', undefined]
    );
  }

  protected static async findDataPtr(
    key: string,
    cache?: boolean
  ): Promise<[DataPtr, string | undefined]> {
    const blyat = await KVManager.findKeyHeader(key, false);
    const [hdrTag, hdr] = blyat;
    if (hdr == undefined) return [{ tag: -1, id: '' }, undefined];

    return [expandDP2(hdr.dataptr[key]), hdrTag];
  }

  protected static async incrementID(): Promise<number> {
    delete headerCache['header0'];
    return (
      await kv.transactWithResult<KVMHeader, number>(
        'header0',
        (hdr = newhdr) => {
          if (hdr == undefined) {
            return {
              next: { ...newhdr, nextID: newhdr.nextID + 1 },
              result: newhdr.nextID
            };
          }
          return {
            next: { ...hdr, nextID: hdr.nextID + 1 },
            result: hdr.nextID
          };
        }
      )
    ).result;
  }

  // Creates/updates a header entry for a key
  protected static async updateKeyHdr(hdrTag: string, k: string, dp: DataPtr) {
    delete headerCache[hdrTag];
    kv.transact<KVMHeader>(hdrTag, (hdr = newhdr) => {
      let ret = {
        ...hdr,
        dataptr: { ...hdr.dataptr, [k]: compressDataPtr(dp) }
      };
      return ret;
    });
  }

  // Deletes a key's header entry
  protected static async deleteKeyHdr(hdrTag: string, key: string) {
    delete headerCache[hdrTag];
    kv.transact<KVMHeader>(hdrTag, (hdr) => {
      if (hdr == undefined) return;
      if (hdrTag != 'header0' && Object.keys(hdr.dataptr).length == 1) return;
      let ret = { ...hdr, dataptr: { ...hdr.dataptr } };
      delete ret.dataptr[key];
      return ret;
    });
  }

  // Adds blocks to header if needed. Only first header has a blocklist
  protected static async addBlock(dataBlock: number) {
    delete headerCache['header0'];
    kv.transact<KVMHeader>('header0', (hdr = newhdr) => {
      if (!(dataBlock in hdr.blocks)) {
        return { ...hdr, blocks: [...hdr.blocks, dataBlock] };
      }
      return hdr;
    });
  }

  // Remove a block from the header.
  protected static async removeBlock(dataBlock: number) {
    delete headerCache['header0'];
    kv.transact<KVMHeader>('header0', (hdr = newhdr) => {
      if (dataBlock in hdr.blocks) {
        let ret = { ...hdr, blocks: [...hdr.blocks] };
        ret.blocks.splice(ret.blocks.indexOf(dataBlock), 1);
        return ret;
      }
    });
  }

  // Updates a key within a tag.
  protected static async updateKeyTag(dptr: DataPtr, v: item_type) {
    delete dataCache[dataPrefix + dptr.tag];
    kv.transact<KVDataTag>(dataPrefix + dptr.tag, (prev = emptyTag) => {
      let ret = { ...prev, data: { ...prev.data, [dptr.id]: v } };
      ret.size = sizeof(ret);
      return ret;
    });
  }

  // Deletes a key within a tag
  protected static async deleteKeyTag(dptr: DataPtr) {
    const tag = dataPrefix + dptr.tag;
    delete dataCache[tag];
    await kv.transact<KVDataTag>(tag, (prev) => {
      if (prev == undefined) return;
      if (Object.keys(prev.data).length == 1) return undefined;
      let ret = { ...prev, data: { ...prev.data } };
      delete ret.data[dptr.id];
      ret.size = sizeof(ret);
      return ret;
    });
    const datum = await kv.get<KVDataTag>(tag);
    if (datum == undefined) {
      KVManager.removeBlock(dptr.tag);
    }
  }

  protected static async getData(dptr: DataPtr, cache?: boolean) {
    const tag = dataPrefix + dptr.tag;
    if (cache) {
      if (tag in dataCache) return dataCache[tag];
    }
    const datum = await kv.get<KVDataTag>(tag);
    if (datum) dataCache[tag] = datum;
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

    return NotPromiseAny(
      hdr.blocks,
      async (blockNum) => {
        const datum = await kv.get<KVDataTag>(dataPrefix + blockNum);
        if ((datum ? datum.size : 0) + size < MAX_TAG_SIZE) return blockNum;
      },
      async () => {
        const newTagNum =
          hdr.blocks.length > 0 ? Math.max(...hdr.blocks) + 1 : 0;
        if (!(newTagNum in hdr.blocks)) KVManager.addBlock(newTagNum);
        return newTagNum;
      }
    );
  }

  protected static async findEmptyHdr(size: number): Promise<string> {
    const getNewHeader = async (headerNums: number[]): Promise<string> => {
      for (let i = 1; i < headerNums.length; ++i) {
        const v = headerNums[i] < 0 ? -headerNums[i] : headerNums[i];
        if (v < headerNums.length && headerNums[v] > 0) headerNums[v] *= -1;
      }
      for (let i = 1; i < headerNums.length; ++i)
        if (headerNums[i] > 0) return headerPrefix + i;
      return headerPrefix + headerNums.length;
    };
    const headers = (await kv.list({ from: headerPrefix })).filter((tag) =>
      tag.startsWith(headerPrefix)
    );

    return NotPromiseAny(
      headers,
      async (hdrTag) => {
        const hdr = await KVManager.getHeader(hdrTag);
        if (sizeof(hdr) + size < MAX_TAG_SIZE) return hdrTag;
      },
      async () => getNewHeader(headers.map((str) => hdrNum(str)))
    );
  }

  static async get(key: string, cache?: boolean) {
    const [dptr, hdrFound] = await KVManager.findDataPtr(key, cache);
    return hdrFound ? KVManager.getData(dptr, cache) : undefined;
  }

  // Cases:
  //  Key doesn't exist yet
  //  Key already exists & enough space
  //  Key doesn't exist and needs to start a new tag
  //  Key already exists but needs to be moved to a new tag
  // Potentially 4 await calls, though I should only need 2 of them.
  //  - findDataPtr & findEmptyTag will look through all tags, cache them?
  static async set(key: string, value: pylon.Json) {
    let [dptr, hdrTag] = await KVManager.findDataPtr(key);
    let findNewTag = true;

    // Check for in-place update case. Its simpler.
    if (hdrTag) {
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
      if (hdrTag == undefined) {
        const hdrSize = sizeof({ [key]: dptr });
        hdrTag = await KVManager.findEmptyHdr(hdrSize);
      }
      KVManager.updateKeyHdr(hdrTag, key, dptr);
    }

    KVManager.updateKeyTag(dptr, value);
  }

  static async delete(key: string) {
    const [dptr, hdrTag] = await KVManager.findDataPtr(key);
    if (hdrTag == undefined) return;

    KVManager.deleteKeyHdr(hdrTag, key);
    KVManager.deleteKeyTag(dptr);
  }

  static async tagsUsed() {
    return kv.count();
  }

  static async listKeys(): Promise<string[]> {
    const headers = (await kv.list({ from: headerPrefix })).filter((tag) =>
      tag.startsWith(headerPrefix)
    );
    const keys = await Promise.all(
      headers.map(async (hdrTag) => {
        const hdr = await KVManager.getHeader(hdrTag);
        return Object.keys(hdr.dataptr);
      })
    );
    return keys.flat(1);
  }

  static async numKeys() {
    return (await KVManager.listKeys()).length;
  }

  static async clear() {
    return kv.clear();
  }

  // Debug helper functions
  static async items() {
    return kv.items();
  }

  static showCache() {
    const z1 = Object.keys(headerCache);
    const z2 = Object.keys(dataCache);
    return z1.concat(z2);
  }

  static async testfn() {
    return KVManager.showCache();
  }
}

export default KVManager;
