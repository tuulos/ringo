import ringogw


def ringo_reader(fd, sze, fname):
        import struct, zlib
        MAGIC_HEAD = (0x47da66b5,)
        MAGIC_TAIL = (0xacc50f5d,)
        def read_really(s):
                t = 0
                buf = ""
                while t < s:
                        r = fd.read(s - t)
                        if not r:
                                return buf
                        t += len(r)
                        buf += r
                return buf

        def check_body(head_body):
                time, entryid, flags, keycrc, keylen, valcrc, vallen =\
                        struct.unpack("<IIIIIII", head_body)
                tot = keylen + vallen + 4
                body = read_really(tot)
                if len(body) < tot:
                        return False, head_body + body
                key = body[:keylen]
                val = body[keylen:-4]
                if zlib.crc32(key) != keycrc or zlib.crc32(val) != valcrc or\
                        struct.unpack("<I", body[-4:]) != MAGIC_TAIL:
                        return False, head_body + body
                else:
                        return True, (entryid, flags, key, val)

        def read_entry():
                head = read_really(8)
                while len(head) >= 8:
                        if struct.unpack("<I", head[:4]) == MAGIC_HEAD:
                                if len(head) < 36:
                                        head += read_really(36 - len(head))
                                if len(head) < 36:
                                        return None
                                head_crc = struct.unpack("<I", head[4:8])[0]
                                head_body = head[8:36]
                                if zlib.crc32(head_body) == head_crc:
                                        ok, cont = check_body(head_body)
                                        if ok:
                                                return cont
                                        head = cont
                        head = head[1:]
                        if len(head) < 8:
                                head += fd.read(1)
                else:
                        return None

        prev_id = None
        while True:
                entry = read_entry()
                if not entry:
                        break
                entryid, flags, key, val = entry
                if flags & 1 or flags & 2:
                        continue
                if entryid == prev_id:
                        continue
                prev_id = entryid
                yield key, val


def input_domain(ringo_host, name):
        ringo = ringogw.Ringo(ringo_host)
        code, res = ringo.request("/mon/domains/domain?name=" + name)
        if code != 200:
                return []
        urls = []
        for domainid, name, nodeid, chunk, owner, nrepl in res:
                nodename, node = nodeid.split('@')
                urls.append("disco://%s/_ringo/%s/rdomain-%s/data"\
                        % (node, nodename[6:], domainid))
        return urls

if __name__ == "__main__":
        import sys
        print "\n".join(input_domain(sys.argv[1], sys.argv[2]))


