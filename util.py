import os
import codecs
import xattr
import struct
import hashlib

VERBOSE = False
SHA256_LEN = 64
XATTR_KEY = 'user.repo.sha256'


def log(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)


def open_text(*args, **kw):
    return open(*args, encoding='utf8', **kw)


def clean_name(name):
    assert isinstance(name, str), repr(name)
    # use canonical path separator
    if os.path.sep != '/':
        name = name.replace(os.path.sep, '/').strip('/')
    # we don't support these characters in filenames
    name = name.replace('\r', ' ').replace('\n', ' ')
    return name


class write_access(object):
    def __init__(self, fn):
        self.fn = fn
        self.mode = None

    def __enter__(self):
        self.mode = os.stat(self.fn).st_mode
        os.chmod(self.fn, self.mode | 0o600)

    def __exit__(self, type, value, traceback):
        if self.mode is not None:
            os.chmod(self.fn, self.mode)



def link_over(src, dst):
    i = 0
    while True:
        try:
            tmp = '%s.%d' % (dst, i)
            os.link(src, tmp)
        except IOError:
            i += 1
        else:
            break
    os.rename(tmp, dst)


class DummyFile(object):
    def __init__(self, fn):
        self.name = fn

    def write(self, block):
        pass

    def flush(self):
        pass

    def close(self):
        pass


def hash_file(src_fn, tmp=None):
    if tmp is None:
        tmp = DummyFile(src_fn)
    h = hashlib.sha256()
    with open(src_fn, 'rb') as fp:
        size = 0
        while True:
            block = fp.read(20000)
            if not block:
                break
            h.update(block)
            tmp.write(block)
            size += len(block)
        tmp.flush()
    return h.hexdigest(), tmp



def get_xattr_hash(fn):
    try:
        d = xattr.getxattr(fn, XATTR_KEY) or None
    except IOError:
        return None
    d = d.decode('ascii')
    log('found xattr %r' % d)
    if ':' not in d:
        return None
    mtime, _, digest = d.partition(':')
    if len(digest) != SHA256_LEN:
        return None
    try:
        mtime = int(mtime)
    except:
        return None
    if int(os.stat(fn).st_mtime) == mtime:
        log('found good digest')
        return digest
    return None


def get_xattr_mtime(fn):
    try:
        d = xattr.getxattr(fn, XATTR_KEY) or None
    except IOError:
        return None
    d = d.decode('ascii')
    if ':' not in d:
        return None
    mtime, _, digest = d.partition(':')
    if len(digest) != SHA256_LEN:
        return None
    try:
        mtime = int(mtime)
    except:
        return None
    return mtime


def set_xattr_hash(fn, digest, mtime=None):
    # set extended attribute containing hash and mtime
    if mtime is None:
        mtime = os.stat(fn).st_mtime
    d = '%d:%s' % (int(mtime), digest)
    with write_access(fn):
        try:
            xattr.setxattr(fn, XATTR_KEY, d.encode('ascii'))
        except IOError:
            pass


# See docs in http://git-annex.branchable.com/internals/hashing/ and implementation in http://sources.debian.net/src/git-annex/5.20140227/Locations.hs/?hl=408#L408
def annex_hashdirmixed(key):
    hasher = hashlib.md5()
    hasher.update(key)
    digest = hasher.digest()
    first_word = struct.unpack('<I', digest[:4])[0]
    nums = [first_word >> (6 * x) & 31 for x in range(4)]
    letters = ["0123456789zqjxkmvwgpfZQJXKMVWGPF"[i] for i in nums]
    return "%s%s/%s%s/" % (letters[1], letters[0], letters[3], letters[2])

