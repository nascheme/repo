#!/usr/bin/python3
# vim: set ai tw=74 sts=4 sw=4 et:
#
# Content indexed file repository

USAGE = """Usage: %prog [options]

  init
    initialize repo
  import <file> [<file> ...]
    link files into repo, write meta-data
  copy <file> [<file> ...]
    copy files into repo, write meta-data
  pull <repo dir>
    copy in files from other repo and merge
  link <pat> [<pat> ...]
    link mataching files from repo
  ls <pat> [<pat> ...]
    list matching files
  status <pat> [<pat> ...]
  show-deleted
    show objects with no name
  delete
    remove objects with specified IDs
  scrub
    verify hashes
"""

import sys
import os
import tempfile
import collections
import time
import datetime
import fcntl
import json
import util
import re
import copy
import contextlib
import glob
import sqlite3 as sqlite
import pickle
from util import log, debug

# command line options
class OPTIONS:
    dryrun = False
    verbose = 0
    no_alias = False
    cont = None

def ensure_dir(dn):
    if not os.path.exists(dn):
        debug('mkdir %r' % dn)
        os.makedirs(dn)


def prefix_path(prefix, fn):
    name = os.path.join(prefix, fn).strip('/')
    return os.path.normpath(name)


Meta = collections.namedtuple('Meta', 'size mtime')

class Repo(object):
    def __init__(self, fn, readonly=False):
        self.root = fn
        self.readonly = readonly
        # path to temporary directory, used with copying files into repo
        self.tmp_dir = os.path.join(fn, 'tmp')
        # path to object data directory, stores contents of files
        self.obj_abs = os.path.join(fn, 'objects')
        # hash to filename index
        self.index_abs = os.path.join(fn, 'index.txt')
        # hash to meta data index (mtime, size)
        self.meta_abs = os.path.join(fn, 'meta.txt')
        self._index = None
        self._meta = None
        # try if in-memory copy is dirty
        self._changed = False
        # last commit time, for auto-commit
        self._last_commit_time = time.time()

    def init(self):
        if os.path.exists(self.index_abs):
            raise RuntimeError('index file exists')
        with self._lock_write():
            with open(self.index_abs, 'wb') as fp:
                pass
            with open(self.meta_abs, 'wb') as fp:
                pass

    def load(self):
        # load index from disk
        self._index = {}
        self._meta = {}
        with self._lock_read():
            if os.path.exists(self.index_abs):
                with util.open_text(self.index_abs) as fp:
                    for line in fp:
                        name, _, digest = line.rstrip().rpartition(' ')
                        self._index[name] = digest
                with util.open_text(self.meta_abs) as fp:
                    for line in fp:
                        digest, size, mtime = line.strip().split()
                        self._meta[digest] = Meta(size=size, mtime=mtime)

    def commit(self):
        # write index to disk
        if OPTIONS.dryrun:
            return
        if not self._changed:
            return
        if self.readonly:
            raise SystemExit('readonly repo')
        with self._lock_write():
            with open(self.meta_abs + '.tmp', 'w') as fp:
                for digest in sorted(self._meta):
                    meta = self._meta[digest]
                    fp.write('%s %s %s\n' % (digest, meta.size, meta.mtime))
            with open(self.index_abs + '.tmp', 'w') as fp:
                for name in sorted(self._index):
                    fp.write('%s %s\n' % (name, self._index[name]))
            debug('commiting changes')
            os.rename(self.meta_abs + '.tmp', self.meta_abs)
            os.rename(self.index_abs + '.tmp', self.index_abs)
        self._changed = False

    def auto_commit(self):
        if time.time() - self._last_commit_time > 20:
            log('auto commit changes')
            self.commit()
            self._last_commit_time = time.time()

    @contextlib.contextmanager
    def _lock_read(self):
        lockfn = os.path.join(self.root, 'lock')
        with open(lockfn, 'ab') as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    @contextlib.contextmanager
    def _lock_write(self):
        lockfn = os.path.join(self.root, 'lock')
        with open(lockfn, 'ab') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def key(self, digest):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        return 'SHA256/%s/%s/%s' % (digest[:3], digest[3:6], digest[6:])

    def data(self, digest):
        return os.path.join(self.obj_abs, self.key(digest) + '.d')

    def filename_digest(self, fn):
        fn = fn.strip()[:-2]
        parts = fn.split(os.path.sep)
        if parts[-4] != 'SHA256':
            raise ValueError('not a hash path %r' % fn)
        return ''.join(parts[-3:])

    def data_exists(self, digest):
        key_abs = self.data(digest)
        return os.path.exists(key_abs)

    def has_meta(self, digest):
        return digest in self._meta

    def get_meta(self, digest):
        meta = self._meta.get(digest)
        if not meta:
            return {}
        return {'size': meta.size, 'mtime': meta.mtime}

    def add_data(self, digest, filename):
        # add content addressed data to repo
        st = os.stat(filename)
        self._meta[digest] = Meta(size=st.st_size, mtime=st.st_mtime)
        self._changed = True

    def get_name_digest(self, name):
        return self._index.get(name)

    def find_name_size(self, name, size):
        digest = self._index.get(name)
        if digest is None:
            return None
        if self._meta[digest].size == size:
            return digest
        return None

    def get_names(self, digest):
        names = []
        for name, digest2 in self._index.items():
            if digest == digest2:
                names.append(name)
        names.sort()
        return names

    def add_name(self, digest, name, overwrite=False):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        if OPTIONS.dryrun:
            return
        digest2 = self._index.get(name)
        if digest2 == digest:
            pass # already exists
        elif digest2 is None:
            self._index[name] = digest
            self._changed = True
        else:
            if overwrite:
                self._index[name] = digest
                self._changed = True
            else:
                log('file with name %r exists, not overwriting' % name)

    def remove_name(self, digest, name):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        if OPTIONS.dryrun:
            return
        if name not in self._index:
            return
        assert self._index[name] == digest
        del self._index[name]
        self._changed = True

    def remove_meta(self, digest):
        if digest in self._meta:
            del self._meta[digest]
            self._changed = True

    def remove_data(self, digest):
        if digest in self._meta:
            log('meta exists for %s, skipping' % digest)
            return
        fn = self.data(digest)
        if os.path.exists(fn):
            log('unlink %s' % fn)
            if not OPTIONS.dryrun:
                os.unlink(fn)
        else:
            log('data does not exist for %s' % digest)

    def delete_files(self, digests):
        if not digests:
            return
        for digest in digests:
            for name in self.get_names(digest):
                log('delete', name)
                if not OPTIONS.dryrun:
                    del self._index[name]
        self._changed = True

    def delete_names(self, names):
        if not names:
            return
        for name in names:
            if name in self._index:
                log('delete', name)
                if not OPTIONS.dryrun:
                    del self._index[name]
            else:
                log('no file named %r' % name)
        self._changed = True

    def set_names(self, digest, names):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        for name in self.get_names(digest):
            del self._index[name]
        for name in names:
            self._index[name] = digest
        self._changed = True

    def set_names_batch(self, files, digests):
        for digest in digests:
            assert len(digest) == util.SHA256_LEN, repr(digest)
        old_names = set()
        for name, digest in self._index.items():
            if digest in digests:
                old_names.append(name)
        for name in old_names:
            del self._index[name]
        for name, digest in files.items():
            self._index[name] = digest
        self._changed = True

    def rename_file(self, old, new):
        log('renaming %r to %r' % (old, new))
        digest = self._index.get(old)
        if digest is not None:
            del self._index[old]
            self._index[new] = digest
            self._changed = True
        else:
            print('no file named %r' % old)

    def list_files(self):
        """Generate list of all objects in the repo.  Generates hash key
        for each object.
        """
        return self._meta.keys()

    def list_file_names(self):
        """Generate list of all files in the repo.  Generates hash key
        and meta data dictionary pairs.
        """
        return self._index.items()

    def get_deleted(self):
        digests = set(self._index.values())
        for digest in self._meta:
            if digest not in digests:
                yield digest

    def get_sizes(self):
        for name, digest in self._index.items():
            meta = self._meta[digest]
            yield name, digest, meta.size

    def _copy_tmp(self, src_fn):
        # copy external file to temporary file inside repo, store hash
        if not OPTIONS.dryrun:
            ensure_dir(self.tmp_dir)
            tmp = tempfile.NamedTemporaryFile(dir=self.tmp_dir)
        else:
            tmp = None
        digest, tmp = util.hash_file(src_fn, tmp=tmp)
        if not OPTIONS.dryrun:
            mtime = os.stat(src_fn).st_mtime
            util.set_xattr_hash(tmp.name, digest, mtime)
        return digest, tmp


    def _link_in(self, fn, digest):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        key_abs = self.data(digest)
        if not OPTIONS.dryrun:
            util.set_xattr_hash(fn, digest)
            if not os.path.exists(key_abs):
                ensure_dir(os.path.dirname(key_abs))
            else:
                os.unlink(key_abs)
            os.link(fn, key_abs)
            os.chmod(key_abs, 0o444)

    def copy_in(self, src_fn, name, overwrite=False):
        """Copy external file into repo (different filesystem)"""
        if name in self._index and not overwrite:
            log('file with same name exists, skipping %s' % src_fn)
            return
        st = os.stat(src_fn)
        digest = self.find_name_size(name, st.st_size)
        if digest:
            log('file with same name and size exists, skipping %s' % src_fn)
            return
        digest, tmp = self._copy_tmp(src_fn)
        if not self.data_exists(digest):
            log('copy new data %s -> %s' % (digest, name))
            self._link_in(tmp.name, digest)
            self.add_data(digest, tmp.name)
            self.add_name(digest, name, overwrite=overwrite)
        else:
            if name not in self.get_names(digest):
                if not OPTIONS.no_alias:
                    log('file exists, add name %s' % name)
                    self.add_name(digest, name, overwrite=overwrite)
                else:
                    log('file exists, not adding alias %s' % name)
        tmp.close()
        return digest

    def link_in(self, fn, digest):
        """Link external file into repo (save filesystem)"""
        if os.path.exists(self.data(digest)):
            log('link over %s' % fn)
        else:
            log('link new %s' % fn)
        self._link_in(fn, digest)

    def link_to(self, digest, dst_fn):
        """Link repo object to new file"""
        key_abs = self.data(digest)
        dn = os.path.dirname(dst_fn)
        if dn:
            if not OPTIONS.dryrun:
                ensure_dir(dn)
        if os.path.exists(dst_fn):
            log('skip existing %s' % dst_fn)
        else:
            if not OPTIONS.dryrun:
                print('link', dst_fn)
                os.link(key_abs, dst_fn)


    def link_overwrite(self, digest, dst_fn):
        """Link repo object over existing file"""
        key_abs = self.data(digest)
        if os.path.samefile(key_abs, dst_fn):
            log('linked already, skipping %s' % dst_fn)
        else:
            print('link over %s' % dst_fn)
            if not OPTIONS.dryrun:
                with util.write_access(os.path.dirname(dst_fn)):
                    util.link_over(key_abs, dst_fn)


def parse_index(fn):
    index = {}
    with util.open_text(fn) as fp:
        for line in fp:
            name, _, digest = line.rstrip().rpartition(' ')
            index[name] = digest
    return index


def _open_repo(args, **kwargs):
    repo = Repo(args.repo, **kwargs)
    repo.load()
    return repo

def do_init(args):
    repo = Repo(args.repo)
    repo.init()

def do_import(args):
    repo = _open_repo(args)
    def store(src_fn):
        name = prefix_path(args.prefix, src_fn)
        digest2 = repo.get_name_digest(name)
        if digest2 is not None and not args.overwrite:
            log('file with same name exists, skipping %s' % src_fn)
            return
        if not args.overwrite:
            size = os.stat(src_fn).st_size
            digest = repo.find_name_size(name, size)
            if digest:
                log('file with same name and size exists, skipping %s' % src_fn)
                return
        if not args.hash:
            digest = util.get_xattr_hash(src_fn)
        else:
            digest = None
        if digest is None:
            debug('computing digest', src_fn)
            digest, tmp = util.hash_file(src_fn)
            assert len(digest) == util.SHA256_LEN, repr(digest)
        if repo.data_exists(digest):
            key_abs = repo.data(digest)
            if not os.path.samefile(src_fn, key_abs):
                print('link from repo %r' % src_fn)
                if not OPTIONS.dryrun:
                    util.link_over(key_abs, src_fn)
            else:
                print('skip existing %r' % src_fn)
        else:
            print('import', src_fn)
            repo.link_in(src_fn, digest)
            repo.add_data(digest, src_fn)
        # save filename in meta data
        repo.add_name(digest, name, overwrite=args.overwrite)

    for fn in _walk_files(args.files):
        if os.path.isfile(fn):
            store(fn)
        else:
            print('skip non-file', fn)
        repo.auto_commit()
    repo.commit()
    print('done.')


def do_copy(args):
    repo = _open_repo(args)
    for fn in _walk_files(args.files):
        if os.path.isfile(fn):
            name = prefix_path(args.prefix, fn)
            repo.copy_in(fn, name, overwrite=args.overwrite)
            print('copy', name)
        else:
            print('skip non-file', fn)
        repo.auto_commit()
    repo.commit()
    print('done.')


def do_pull(args):
    repo = _open_repo(args)
    for fn in args.other_repo:
        other = Repo(fn, readonly=True)
        other.load()
        for fn, digest in sorted(other.list_file_names()):
            digest2 = repo.get_name_digest(fn)
            if digest2 == digest:
                continue # already exists
            elif digest2 is not None and not args.overwrite:
                log('file with same name exists, skipping %s' % fn)
                continue # don't overwrite, skip
            if repo.data_exists(digest):
                debug('add name', fn, digest)
                repo.add_name(digest, fn, overwrite=args.overwrite)
            else:
                debug('copy in', fn, digest)
                repo.copy_in(other.data(digest), fn, overwrite=args.overwrite)
            repo.auto_commit()
    repo.commit()
    print('done.')


def do_diff(args):
    repo = _open_repo(args)
    for fn in args.other_repo:
        other = Repo(fn, readonly=True)
        other.load()
        for fn, digest in other.list_file_names():
            if repo.has_meta(digest):
                continue
            else:
                if args.meta:
                    meta = other.get_meta(digest)
                    print('%s %s %s' % (digest, meta['size'], meta['mtime']))
                else:
                    print(fn)


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


def annex_fix(args, repo):
    annex = Annex(args[0])

    def annex_obj_path(fn):
        # full path to annex object
        parts = fn.split(os.path.sep)
        return os.path.join(annex.obj_abs, *parts[-4:])

    def annex_digest(fn):
        # SHA256 hexdigest
        key = os.path.basename(fn)
        assert key.startswith('SHA256-'), repr(key)
        _, _, digest = key.rpartition('--')
        return digest

    def do_link(fn, force=False):
        # link repo file into annex objects
        dst = os.readlink(fn)
        if '.git/annex/objects/' in dst and 'SHA256-' in dst:
            digest = annex_digest(dst)
            if repo.data_exists(digest):
                # found file in repo
                obj_fn = annex_obj_path(dst)
                if os.path.exists(obj_fn):
                    if force:
                        repo.link_overwrite(digest, obj_fn)
                    else:
                        log('obj exists, skipping %s' % obj_fn)
                else:
                    repo.link_to(digest, obj_fn)
            else:
                print('not found', fn)

    for fn in _walk_files(args):
        if os.path.islink(fn):
            do_link(fn, force=args.force)
    print('done.')


def annex_add(args, repo):
    annex = Annex(args[0])

    def annex_key(size, digest):
        return 'SHA256-s%d--%s' % (size, digest)

    def annex_obj_path(key):
        d = util.annex_hashdirmixed(key)
        return os.path.join(annex.obj_abs, d, key, key)

    def do_add(fn):
        digest = util.get_xattr_hash(fn)
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
            if not OPTIONS.dryrun:
                ensure_dir(os.path.dirname(key_fn))
                os.link(fn, key_fn)
                os.unlink(fn)
                os.symlink(key_rel, fn)
                print('add', fn)

    for fn in _walk_files(args):
        if os.path.isfile(fn):
            do_add(fn)
    print('done.')


def _build_index(repo):
    index = {}
    for name, digest in repo.list_file_names():
        i = 1
        name = util.clean_name(name)
        path = name
        while path in index:
            path = '%s.%d' % (name, i)
            i += 1
        index[path] = digest
    return index


def do_fix_paths(args):
    repo = _open_repo(args)
    filenames = {}
    digests = set()
    fix = False
    for name, digest in repo.list_file_names():
        if name.startswith('/'):
            print(digest, name)
            fix = True
            name = name.lstrip('/')
        digests.add(digest)
        filenames[name] = digest
    if fix:
        repo.set_names_batch(filenames, digests)
        repo.commit()


def do_ls_known(args):
    repo = _open_repo(args)
    for fn in _walk_files(args.dirs):
        if os.path.isfile(fn):
            digest = util.get_xattr_hash(fn)
            if digest and repo.has_meta(digest):
                print(fn)
                if args.delete:
                    os.unlink(fn)


def do_ls_unknown(args):
    repo = _open_repo(args)
    for fn in _walk_files(args.dirs):
        if os.path.isfile(fn):
            digest = util.get_xattr_hash(fn)
            if not digest or not repo.has_meta(digest):
                print(fn, digest)


def do_fix_times(args):
    repo = _open_repo(args)
    for digest in repo.list_files():
        fn = repo.data(digest)
        meta = repo.get_meta(digest)
        st = os.stat(fn)
        mtime = float(meta['mtime'])
        if abs(mtime - st.st_mtime) >= 1:
            log('fix mtime', digest)
            os.utime(fn, (st.st_atime, mtime))
        xattr_mtime = util.get_xattr_mtime(fn)
        if abs(mtime - (xattr_mtime or 0)) > 1:
            log('xattr mtime mismatch', digest)


def do_cat(args):
    repo = _open_repo(args)
    for digest in args.digests:
        fn = repo.data(digest)
        if os.path.exists(fn):
            sys.stdout.write('%s:\n' % digest)
            with open(fn, 'rb') as fp:
                while True:
                    b = fp.read(10000)
                    if not b:
                        break
                    s = b.decode('utf-8', errors='backslashreplace')
                    sys.stdout.write(s)


def do_scrub(args):
    repo = _open_repo(args)
    problems = 0
    err_file = os.path.join(repo.root, 'scrub_errors.txt')
    t = time.time()
    with util.open_text(err_file, 'a', buffering=1) as err_fp:
        def err(*args):
            print(*args, file=err_fp)
        err('scrub started %s' % datetime.datetime.now())
        if args.cont:
            err('continue from %s' % args.cont)
        for digest in repo.list_files():
            if args.cont:
                if digest == args.cont:
                    args.cont = False
                else:
                    continue # skip
            if time.time() - t > 20:
                # print progress
                print('scrub', digest)
                t = time.time()
            fn = repo.data(digest)
            if not os.path.exists(fn):
                err('missing data', digest)
                continue
            meta = repo.get_meta(digest)
            st = os.stat(fn)
            if st.st_size != int(meta['size']):
                err('size mismatch', digest, meta['size'], st.st_size)
            mtime_differs = abs(st.st_mtime - float(meta['mtime'])) > 5
            if mtime_differs:
                err('mtime differs', digest, meta['mtime'])
            if args.fast:
                continue # no check of hashes
            if args.size and st.st_size > args.size:
                continue # skip large file
            if args.modified and not mtime_differs:
                continue # skip, modified time same
            # do the expensive hash
            digest2, tmp = util.hash_file(fn)
            log(digest, digest2)
            if digest != digest2:
                err('checksum mismatch', digest)
                problems += 1
            else:
                digest2 = util.get_xattr_hash(fn)
                if digest2 != digest:
                    err('update xattr', digest)
                    util.set_xattr_hash(fn, digest)
                    problems += 1
    if problems:
        print('problems were found (%s), see scrub_errors.txt' % problems)


def do_link_files(args):
    import fnmatch
    repo = _open_repo(args)
    index = _build_index(repo)
    log('loaded %d files' % len(index))
    matches = []
    for pat in args.pats:
        for fn in fnmatch.filter(index, pat):
            matches.append((fn, index[fn]))
    matches.sort()
    for dst, src in matches:
        repo.link_to(src, dst)


def do_find_dups(args):
    min_size = args.min_size
    suffixes = {
        'K': 1e3,
        'M': 1e6,
        'G': 1e9,
        'T': 1e12,
        }
    if min_size:
        suffix = min_size[-1]
        if suffix.isdigit():
            min_size = float(min_size)
        else:
            min_size = float(min_size[:-1])
            min_size *= suffixes[suffix.upper()]
    repo = _open_repo(args)
    names = collections.defaultdict(lambda : [])
    for fn, digest, size in sorted(repo.get_sizes()):
        if int(size) >= min_size:
            names[digest].append(fn)
    for digest, fn_list in sorted(names.items(), key=lambda item: item[1]):
        if len(fn_list) > 1:
            print(fn_list[0], digest)
            for fn in fn_list[1:]:
                print('  ', fn)


def do_status(args):
    import fnmatch, glob
    repo = _open_repo(args)
    ok = set()
    removed = set()
    added = set()
    changed = set()
    index = _build_index(repo)
    log('loaded %d files' % len(index))
    for pat in args.pats:
        for fn, digest in index.items():
            if fnmatch.fnmatch(fn, pat):
                if os.path.exists(fn):
                    key_abs = repo.data(digest)
                    if os.path.samefile(key_abs, fn):
                        ok.add(fn)
                    else:
                        changed.add(fn)
                else:
                    removed.add(fn)
        for fn in glob.glob(pat.replace('*', '**'), recursive=True):
            if os.path.isdir(fn):
                continue
            if fn in ok:
                pass
            elif fn in changed:
                pass
            else:
                added.add(fn)
    if removed:
        print('Removed:')
        for fn in sorted(removed):
            print('    %s' % fn)
    if added:
        print('Added:')
        for fn in sorted(added):
            print('    %s' % fn)
    if changed:
        print('Changed:')
        for fn in sorted(changed):
            print('    %s' % fn)


def do_list_files(args):
    import fnmatch
    repo = _open_repo(args)
    for name, digest in repo.list_file_names():
        for pat in args.pats:
            if fnmatch.fnmatch(name.lower(), pat):
                if args.long:
                    print(name, digest)
                else:
                    print(name)
                break


class PathInfo:
    def __init__(self, path, parent=None):
        self.path = path
        self.parent = parent
        self.children = []
        self.size = 0

def do_du_save(args, repo):
    if len(args) != 1:
        raise SystemExit('need one arg')
    import pickle
    files = {}
    def get(fn):
        if fn in files:
            return files[fn]
        dn = os.path.dirname(fn)
        if dn != fn:
            parent = get(dn)
        else:
            parent = None
        files[fn] = p = PathInfo(fn, parent)
        if parent:
            parent.children.append(p)
        return p
    for name, digest, size in repo.get_sizes():
        if not name.startswith('/'):
            name = '/' + name
        p = get(name)
        p.size = size
    root = files.get('/') or files.get('.')
    root.parent = root
    with open(args[0], 'wb') as fp:
        pickle.dump(root, fp, protocol=2)


def do_show_deleted(args):
    repo = _open_repo(args)
    if not args.crawl:
        deleted = repo.get_deleted()
    else:
        deleted = []
        for dn, dirs, files in os.walk(repo.obj_abs):
            for fn in files:
                digest = repo.filename_digest(os.path.join(dn, fn))
                if not repo.has_meta(digest):
                    deleted.append(digest)
    for digest in list(deleted):
        if args.path:
            print(repo.data(digest))
        else:
            print(digest)
        if args.clean:
            repo.remove_meta(digest)
            repo.remove_data(digest)
    if args.clean:
        repo.commit()


def do_clean_meta(args):
    repo = _open_repo(args)
    for digest in list(repo.get_deleted()):
        repo.remove_meta(digest)
    repo.commit()

def do_clean_missing(args):
    repo = _open_repo(args)
    delete = set()
    for digest in repo.list_files():
        if not repo.data_exists(digest):
            log('removing', digest)
            delete.add(digest)
    repo.delete_files(delete)
    repo.commit()

def do_delete(args):
    repo = _open_repo(args)
    digests = list(args.digests)
    if args.infile:
        log('reading digests from file %r' % args.infile)
        with open(args.infile) as fp:
            for line in fp:
                digests.append(line.strip())
    log('deleting %d files' % len(digests))
    repo.delete_files(digests)
    repo.commit()

def do_delete_names(args):
    import fnmatch
    repo = _open_repo(args)
    names = list(args.names)
    if args.infile:
        log('reading names from file %r' % args.infile)
        with open(args.infile) as fp:
            for line in fp:
                names.append(line.strip())
    log('deleting %d files' % len(names))
    repo.delete_names(names)
    repo.commit()

def do_delete_patterns(args):
    repo = _open_repo(args)
    matches = set()
    for pat in args.patterns:
        pat = re.compile(pat)
        for name in repo._index:
            if pat.match(name):
                matches.add(name)
    repo.delete_names(matches)
    repo.commit()

def do_rename_files(args):
    repo = _open_repo(args)
    prefix = args.prefix
    with open(args.rename_list, 'r') as fp:
        for line in fp:
            old, sep, new = line.partition('\t')
            new = new[:-1]
            if prefix:
                old = prefix + old
                new = prefix + new
            repo.rename_file(old, new)
    repo.commit()

def main():
    global OPTIONS
    import argparse
    import logging
    parser = argparse.ArgumentParser(prog='repo.py')
    subparsers = parser.add_subparsers()

    parser.add_argument('--repo', '-r', default=None)
    parser.add_argument('--dryrun', '-n', default=False,
                        action="store_true",
                        help="print actions, do not change anything")
    parser.add_argument('-v', '--verbose',
                        action='count', dest='verbose', default=0,
                        help="enable extra status output")
    parser.add_argument('-d', '--debug',
                        action='count', dest='debug', default=0,
                        help="enable debugging output")

    add_sub = subparsers.add_parser

    sub = add_sub('init',
                  help='initialize a new repository')
    sub.set_defaults(func=do_init)

    sub = add_sub('import',
                  help='link files into repo')
    sub.add_argument('--prefix', '-p', default='/')
    sub.add_argument('--overwrite', default=False,
                     action='store_true',
                     help='overwrite existing names in repo with new data')
    sub.add_argument('--hash', default=False,
                     action='store_true',
                     help='Do not trust xattr hash info, rehash file.')
    sub.add_argument('files', nargs='*')
    sub.set_defaults(func=do_import)

    sub = add_sub('copy',
                  help='copy files into repo')
    sub.add_argument('--prefix', '-p', default='/')
    sub.add_argument('--overwrite', default=False,
                     action='store_true',
                     help='overwrite existing names in repo with new data')
    sub.add_argument('files', nargs='*')
    sub.set_defaults(func=do_copy)

    sub = add_sub('pull',
                  help='pull files from other repo')
    sub.add_argument('--overwrite', default=False,
                     action='store_true',
                     help='overwrite existing names in repo with new files')
    sub.add_argument('other_repo', nargs='*')
    sub.set_defaults(func=do_pull)

    sub = add_sub('diff',
                  help='compare files with other repo, print new in other')
    sub.add_argument('--meta', default=False,
                     action='store_true',
                     help='print meta data line')
    sub.add_argument('other_repo', nargs='*')
    sub.set_defaults(func=do_diff)

    sub = add_sub('ls',
                  help='list matching files')
    sub.add_argument('--long', '-l', default=False,
                     action='store_true',
                     help='show names and hashes')
    sub.add_argument('pats', nargs='*')
    sub.set_defaults(func=do_list_files)

    sub = add_sub('link',
                  help='link match files from repo')
    sub.add_argument('pats', nargs='*')
    sub.set_defaults(func=do_link_files)

    sub = add_sub('known',
                  help='find files that are stored in repo')
    sub.add_argument('--delete', '-d', default=False, action='store_true',
                     help='delete files that are known in repo')
    sub.add_argument('dirs', nargs='*')
    sub.set_defaults(func=do_ls_known)

    sub = add_sub('unknown',
                  help='find files that are not stored in repo')
    sub.add_argument('dirs', nargs='*')
    sub.set_defaults(func=do_ls_unknown)

    sub = add_sub('status',
                  help='show status of linked files (new, removed, changed)')
    sub.add_argument('pats', nargs='*')
    sub.set_defaults(func=do_status)

    sub = add_sub('find-dups',
                  help='show duplicated files (more than one link)')
    sub.add_argument('--min-size', '-s', default=0,
                     help='minimum size of duplicate')
    sub.set_defaults(func=do_find_dups)

    sub = add_sub('show-deleted',
                  help='list objects in repo with no name')
    sub.add_argument('--path', '-p', default=False, action='store_true',
                     help='show data path rather than hash')
    sub.add_argument('--crawl', '-c', default=False, action='store_true',
                     help='crawl data files rather than using meta db')
    sub.add_argument('--clean', default=False, action='store_true',
                     help='remove meta and file data for deleted items')
    sub.set_defaults(func=do_show_deleted)

    sub = add_sub('delete',
                  help='delete specified objects from repo')
    sub.add_argument('--infile', '-i', default=None,
                     help='file containing digests, one per line')
    sub.add_argument('digests', nargs='*')
    sub.set_defaults(func=do_delete)

    sub = add_sub('delete-names',
                  help='delete specified named files')
    sub.add_argument('--infile', '-i', default=None,
                     help='file containing filenames, one per line')
    sub.add_argument('names', nargs='*')
    sub.set_defaults(func=do_delete_names)

    sub = add_sub('delete-patterns',
                  help='delete specified named files (by regex pattern)')
    sub.add_argument('patterns', nargs='*')
    sub.set_defaults(func=do_delete_patterns)

    sub = add_sub('rename-files',
                  help='rename files, input is tab separated (old -> new)')
    sub.add_argument('--prefix', '-p', default='',
                     help='prefix path to append to all names')
    sub.add_argument('rename_list',
                     help='rename actions, one line per file, tab separated')
    sub.set_defaults(func=do_rename_files)

    sub = add_sub('clean-meta',
                  help='remove objects from meta DB that do not exist')
    sub.set_defaults(func=do_clean_meta)

    sub = add_sub('clean-missing',
                  help='remove objects from index that do not exist on disk')
    sub.set_defaults(func=do_clean_missing)

    sub = add_sub('fix-times',
                  help='repair file modification times from meta data')
    sub.set_defaults(func=do_fix_times)

    sub = add_sub('fix-paths',
                  help='remove leading slash from paths')
    sub.set_defaults(func=do_fix_paths)

    sub = add_sub('cat',
                  help='given hashes, print data to stdout')
    sub.add_argument('digests', nargs='*')
    sub.set_defaults(func=do_cat)


    sub = add_sub('scrub',
                  help='verify hashes in repo')
    sub.add_argument('--continue', '-c', default=None, dest='cont',
                     help='continue from digest')
    sub.add_argument('--size', '-s', default=None, type=int,
                     help='only check objects smaller than this size')
    sub.add_argument('--modified', '-m', default=False, action='store_true',
                     help='only check objects with changed times')
    sub.add_argument('--fast', '-f', default=False, action='store_true',
                     help='only check sizes, skip rehashing file data')
    sub.set_defaults(func=do_scrub)

    args = parser.parse_args()
    if not args.repo or not hasattr(args, 'func'):
        parser.print_help()
        return
    util.VERBOSE = args.verbose
    util.DEBUG = args.debug
    OPTIONS.dryrun = args.dryrun
    os.umask(0o002)
    args.func(args)

if __name__ == '__main__':
    main()
