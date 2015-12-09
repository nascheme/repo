#!/usr/bin/python3
# vim: set ai tw=74 sts=4 sw=4 et:
#
#




USAGE = "Usage: %prog [options]"

import sys
import os
import logging
import tempfile
import collections
import fcntl
import json
import hashlib
import struct
import codecs
import xattr

DRY_RUN = False
VERBOSE = False
LINK = False
FORCE = False

def log(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs)

def ensure_dir(dn):
    if not os.path.exists(dn):
        log('mkdir %r' % dn)
        os.makedirs(dn)


def _read_meta(key_abs):
    with open(key_abs, 'rb') as key:
        fcntl.flock(key, fcntl.LOCK_SH)
        meta = key_abs.replace('.d', '.m')
        if not os.path.exists(meta):
            data = {}
        else:
            with codecs.open(meta, 'r', encoding='utf8') as fp:
                data = json.load(fp)
        fcntl.flock(key, fcntl.LOCK_UN)
    return data


_SHA256_LEN = 64

class Repo(object):
    def __init__(self, fn):
        self.root = fn
        self.tmp_dir = os.path.join(fn, 'tmp')
        self.obj_abs = os.path.join(fn, 'objects')

    def key(self, digest):
        assert len(digest) == _SHA256_LEN
        return 'SHA256/%s/%s/%s' % (digest[:3], digest[3:6], digest[6:])

    def data(self, digest):
        return os.path.join(self.obj_abs, self.key(digest) + '.d')

    def meta(self, digest):
        return os.path.join(self.obj_abs, self.key(digest) + '.m')

    def exists(self, digest):
        key_abs = self.data(digest)
        return os.path.exists(key_abs)

    def _copy_tmp(self, src_fn):
        # copy external file to temporary file inside repo, store hash
        if not DRY_RUN:
            ensure_dir(self.tmp_dir)
            tmp = tempfile.NamedTemporaryFile(dir=self.tmp_dir)
        else:
            tmp = None
        digest, tmp = _hash_file(src_fn, tmp=tmp)
        if not DRY_RUN:
            mtime = os.stat(src_fn).st_mtime
            _set_xattr_hash(tmp.name, digest, mtime)
        return digest, tmp


    def copy_in(self, src_fn, name):
        """Copy external file into repo (different filesystem)"""
        digest, tmp = self._copy_tmp(src_fn)
        if self.exists(digest):
            tmp.close() # discard
        else:
            log('copy', src_fn)
            self.link_in(tmp.name, digest)
            self.write_meta(digest, name)
        return digest


    def link_in(self, fn, digest):
        """Link external file into repo (save filesystem)"""
        assert len(digest) == _SHA256_LEN
        key_abs = self.data(digest)
        if os.path.exists(key_abs):
            return
        if not DRY_RUN:
            ensure_dir(os.path.dirname(key_abs))
            os.link(fn, key_abs)
            _set_xattr_hash(key_abs, digest)
            os.chmod(key_abs, 0o400)


    def link_to(self, digest, dst_fn):
        """Link repo object to new file"""
        key_abs = self.data(digest)
        if not DRY_RUN:
            ensure_dir(os.path.dirname(dst_fn))
            os.link(key_abs, dst_fn)


    def link_overwrite(self, digest, dst_fn):
        """Link repo object over existing file"""
        key_abs = self.data(digest)
        if os.path.samefile(key_abs, dst_fn):
            log('linked already, skipping %s' % dst_fn)
        else:
            print('link over %s' % dst_fn)
            if not DRY_RUN:
                with write_access(os.path.dirname(dst_fn)):
                    _link_over(key_abs, dst_fn)


    def read_meta(self, digest):
        key_abs = self.data(digest)
        return _read_meta(key_abs)


    def write_meta(self, digest, name):
        assert len(digest) == _SHA256_LEN
        if DRY_RUN:
            return
        key_abs = self.data(digest)
        with open(key_abs, 'rb') as key:
            fcntl.flock(key, fcntl.LOCK_EX)
            meta = key_abs.replace('.d', '.m')
            if os.path.exists(meta):
                with codecs.open(meta, 'r', encoding='utf8') as fp:
                    data = json.load(fp)
            else:
                data = {}
            old_data = data.copy()
            names = data.get('names', [])
            #name = try_decode(name)
            assert isinstance(name, str)
            if name not in names:
                names.append(name)
            names.sort()
            data['names'] = names
            data['version'] = 1
            st = os.stat(key_abs)
            data['size'] = st.st_size
            mtime = int(st.st_mtime)
            if 'mtime' not in data or data['mtime'] > mtime:
                data['mtime'] = mtime
            if data != old_data:
                with codecs.open(meta + '.new', 'w', encoding='utf8') as fp:
                    log('write meta %s' % data)
                    json.dump(data, fp, indent=4)
                os.rename(meta + '.new', meta)
            fcntl.flock(key, fcntl.LOCK_UN)


def try_decode(name):
    if isinstance(name, str):
        try:
            name = name.decode('utf8')
        except UnicodeDecodeError:
            name = name.decode('latin1')
    return name


_XATTR_KEY = 'user.repo.sha256'


def _get_xattr_hash(fn):
    try:
        d = xattr.getxattr(fn, _XATTR_KEY) or None
    except IOError:
        return None
    d = d.decode('ascii')
    log('found xattr %r' % d)
    if ':' not in d:
        return None
    mtime, _, digest = d.partition(':')
    if len(digest) != _SHA256_LEN:
        return None
    try:
        mtime = int(mtime)
    except:
        return None
    if int(os.stat(fn).st_mtime) == mtime:
        log('found good digest')
        return digest
    return None


def _set_xattr_hash(fn, digest, mtime=None):
    # set extended attribute containing hash and mtime
    if mtime is None:
        mtime = os.stat(fn).st_mtime
    d = '%d:%s' % (int(mtime), digest)
    with write_access(fn):
        try:
            xattr.setxattr(fn, _XATTR_KEY, d.encode('ascii'))
        except IOError:
            pass


class DummyFile(object):
    def __init__(self, fn):
        self.name = fn

    def write(self, block):
        pass

    def flush(self):
        pass

    def close(self):
        pass


def _hash_file(src_fn, tmp=None):
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


def _link_over(src, dst):
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


def do_import(args, repo, prefix):

    def store(src_fn):
        digest = attr_digest = _get_xattr_hash(src_fn)
        if digest is None:
            log('computing digest', src_fn)
            digest, tmp = _hash_file(src_fn)
            assert len(digest) == _SHA256_LEN
        if repo.exists(digest):
            key_abs = repo.data(digest)
            if not os.path.samefile(src_fn, key_abs):
                print('link over %r' % src_fn)
                if not DRY_RUN:
                    _link_over(key_abs, src_fn)
            else:
                print('skip existing %r' % src_fn)
        else:
            print('import', src_fn)
            repo.link_in(src_fn, digest)
        # save filename in meta data
        repo.write_meta(digest, os.path.join(prefix, src_fn))

    for fn in _walk_files(args):
        if os.path.isfile(fn):
            store(fn)
        else:
            print('skip non-file', fn)
    print('done.')


def do_copy(args, dst_dir, repo, prefix):

    def copy(fn):
        dst_fn = os.path.join(dst_dir, fn)
        if os.path.exists(dst_fn):
            print('skip existing', dst_fn)
        else:
            name = os.path.join(prefix, fn)
            digest = repo.copy_in(fn, name)
            print('link', dst_fn)
            repo.link_to(digest, dst_fn)

    for fn in _walk_files(args):
        if os.path.isfile(fn):
            copy(fn)
        else:
            print('skip non-file', fn)
    print('done.')


def _find_git(fn):
    dirname = fn
    depth = 0
    while True:
        if os.path.isdir(os.path.join(dirname, '.git')):
            return dirname, depth
        dirname = os.path.dirname(dirname)
        depth += 1
        if len(os.path.split(dirname)) < 1:
            raise SystemExit('cannot find .git')


def _object_path(dst):
    git_dir, depth = _find_git(dst)
    objects = os.path.join(git_dir, '.git', 'annex', 'objects')
    if not os.path.isdir(objects):
        raise SystemExit("%r doesn't exist" % objects)
    return objects


class Annex(object):
    def __init__(self, start_dir):
        self.root, self.depth = _find_git(start_dir)
        self.obj_abs = _object_path(start_dir)
        self.tmp_dir = os.path.join(self.root, '.git', 'annex', 'misctmp')


_SKIP_DIRS = set(['.git'])

def _walk_files(args):
    todo = collections.deque(args)
    while todo:
        fn = todo.popleft()
        if os.path.isdir(fn):
            for sub in os.listdir(fn):
                if sub in _SKIP_DIRS:
                    continue
                todo.appendleft(os.path.normpath(os.path.join(fn, sub)))
        else:
            yield fn


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


def annex_fix(args, repo):
    annex = Annex(args[0])

    def annex_obj_path(fn):
        # full path to annex object
        parts = fn.split(os.path.sep)
        return os.path.join(annex.obj_abs, *parts[-4:])

    def annex_digest(fn):
        # SHA256 hexdigest
        key = os.path.basename(fn)
        assert key.startswith('SHA256-')
        _, _, digest = key.rpartition('--')
        return digest

    def do_link(fn):
        # link repo file into annex objects
        dst = os.readlink(fn)
        if '.git/annex/objects/' in dst and 'SHA256-' in dst:
            digest = annex_digest(dst)
            if repo.exists(digest):
                # found file in repo
                obj_fn = annex_obj_path(dst)
                if os.path.exists(obj_fn):
                    if FORCE:
                        repo.link_overwrite(digest, obj_fn)
                    else:
                        log('obj exists, skipping %s' % obj_fn)
                else:
                    print('link %s' % obj_fn)
                    repo.link_to(digest, obj_fn)
            else:
                print('not found', fn)

    for fn in _walk_files(args):
        if os.path.islink(fn):
            do_link(fn)
    print('done.')


# See docs in http://git-annex.branchable.com/internals/hashing/ and implementation in http://sources.debian.net/src/git-annex/5.20140227/Locations.hs/?hl=408#L408
def _annex_hashdirmixed(key):
    hasher = hashlib.md5()
    hasher.update(key)
    digest = hasher.digest()
    first_word = struct.unpack('<I', digest[:4])[0]
    nums = [first_word >> (6 * x) & 31 for x in range(4)]
    letters = ["0123456789zqjxkmvwgpfZQJXKMVWGPF"[i] for i in nums]
    return "%s%s/%s%s/" % (letters[1], letters[0], letters[3], letters[2])

def annex_add(args, repo):
    annex = Annex(args[0])

    def annex_key(size, digest):
        return 'SHA256-s%d--%s' % (size, digest)

    def annex_obj_path(key):
        d = _annex_hashdirmixed(key)
        return os.path.join(annex.obj_abs, d, key, key)

    def do_add(fn):
        digest = _get_xattr_hash(fn)
        if digest is None:
            print('missing xattr', fn)
            return
        size = os.stat(fn).st_size
        key = annex_key(size, digest)
        key_fn = annex_obj_path(key)
        key_abs = os.path.abspath(key_fn)
        if not os.path.exists(key_fn):
            key_rel = os.path.relpath(key_abs,
                                      os.path.abspath(os.path.dirname(fn)))
            log('symlink', key_rel, '->', fn)
            if not DRY_RUN:
                ensure_dir(os.path.dirname(key_fn))
                os.link(fn, key_fn)
                os.unlink(fn)
                os.symlink(key_rel, fn)
                print('add', fn)

    for fn in _walk_files(args):
        if os.path.isfile(fn):
            do_add(fn)
    print('done.')


def do_index(repo):
    index = {}
    for fn in _walk_files([repo.obj_abs]):
        if fn.endswith('.d'):
            data = _read_meta(fn)
            names = data.get('names') or []
            for name in names:
                i = 1
                path = name
                while path in index:
                    path = '%s.%d' % (name, i)
                    i += 1
                rel = os.path.relpath(fn, repo.obj_abs)
                log(path, rel)
                index[path] = rel
    out_fn = os.path.join(repo.root, 'index.txt')
    with codecs.open(out_fn + '.new', 'w', encoding='utf8') as fp:
        for path in sorted(index):
            fp.write('%s %s\n' % (path, index[path]))
    os.rename(out_fn + '.new', out_fn)


def main():
    global DRY_RUN, VERBOSE, LINK, FORCE

    import optparse
    import logging
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--repo', '-r', default=None)
    parser.add_option('--prefix', '-p', default=None)
    parser.add_option('--dst', '-d', default=None)
    parser.add_option('--force', '-f', default=False,
                     action="store_true",
                     help="force overwrite")
    parser.add_option('--dryrun', '-n', default=False,
                      action="store_true",
                      help="print actions, do not change anything")
    parser.add_option('-v', '--verbose',
                      action='count', dest='verbose', default=0,
                      help="enable extra status output")
    options, args = parser.parse_args()
    action = args[0]
    args = args[1:]
    level = logging.WARNING # default
    if options.verbose == 1:
        level = logging.INFO
    elif options.verbose > 1:
        level = logging.DEBUG
    DRY_RUN = options.dryrun
    VERBOSE = options.verbose
    FORCE = options.force
    #LINK = options.link
    logging.basicConfig(level=level)
    if not options.repo:
        raise SystemExit('need --repo option')
    repo = Repo(options.repo)
    if action == 'import':
        # compute content-ids, link into repo, write meta-data
        do_import(args, repo, options.prefix)
    elif action == 'copy':
        # copy files from another device, then import
        do_copy(args, options.dst, repo, options.prefix)
    elif action == 'index':
        do_index(repo)
    elif action == 'annex-fix':
        # crawl symlinks, link found objects into .git/annex/objects
        annex_fix(args, repo)
    elif action == 'annex-add':
        # crawl files, add to annex objects, replace file with symlink
        annex_add(args, repo)
    else:
        raise SystemExit('unknown action %r' % action)

if __name__ == '__main__':
    main()
