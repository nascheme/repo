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
  link <pat> [<pat> ...]
    link mataching files from repo
  ls <pat> [<pat> ...]
    list matching files
  index
    create index file
  scrub
    verify hashes
"""

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
from util import log

DRY_RUN = False
LINK = False
FORCE = False
CONTINUE = None
NO_ALIAS = False

_DB_PRAGMA = '''\
PRAGMA synchronous=OFF;
PRAGMA cache_size=200000;
'''

_DB_SCHEMA = '''\
begin transaction;
create table files (
    digest text primary key,
    size integer,
    mtime integer
    );
create table names (
    digest text,
    name text,
    primary key (digest, name)
    );
commit;
'''

def ensure_dir(dn):
    if not os.path.exists(dn):
        log('mkdir %r' % dn)
        os.makedirs(dn)


class Repo(object):
    def __init__(self, fn):
        self.root = fn
        self.tmp_dir = os.path.join(fn, 'tmp')
        self.obj_abs = os.path.join(fn, 'objects')
        self.idx_abs = os.path.join(fn, 'index.db')
        self._changed = False
        if os.path.exists(self.idx_abs):
            self.conn = sqlite.connect(self.idx_abs)
            self._pragma()
        else:
            self.conn = None

    def _pragma(self):
        #c = self.conn.cursor()
        #c.execute(_DB_PRAGMA)
        self.conn.executescript(_DB_PRAGMA)

    def init(self):
        self.conn = sqlite.connect(self.idx_abs)
        self._pragma()
        self.conn.executescript(_DB_SCHEMA)

    def commit(self):
        if self._changed:
            self.conn.commit()
            self._changed = False

    @contextlib.contextmanager
    def lock_read(self):
        lockfn = os.path.join(self.root, 'lock')
        with open(lockfn, 'ab') as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    @contextlib.contextmanager
    def lock_write(self):
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
        return ''.join(parts[-3:])

    def exists(self, digest):
        key_abs = self.data(digest)
        return os.path.exists(key_abs)

    def get_meta(self, digest):
        c = self.conn.cursor()
        c.execute('select size, mtime from files'
                  ' where digest=?', (digest,))
        if c.rowcount > 0:
            size, mtime = c.fetchone()
            return {'size': size, 'mtime': mtime}
        return {}

    def add_file(self, digest, filename):
        st = os.stat(filename)
        self.conn.execute('insert or ignore into files (digest, size, mtime)'
                          ' values (?, ?, ?)',
                          (digest, st.st_size, st.st_mtime))
        self._changed = True

    def find_name_size(self, name, size):
        c = self.conn.cursor()
        c.execute('select digest from names'
                  ' where name=?',
                  (name,))
        digest, = c.fetchone() or [None]
        c.execute('select size from files'
                  ' where digest=?',
                  (digest,))
        other_size, = c.fetchone() or [-1]
        if size == other_size:
            return digest
        return None

    def get_names(self, digest):
        c = self.conn.cursor()
        c.execute('select (name) from names'
                  ' where digest=?'
                  ' order by name',
                  (digest,))
        return c.fetchall()

    def add_name(self, digest, name):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        if DRY_RUN:
            return
        self.conn.execute('insert or ignore into names'
                          ' values (?, ?)', (digest, name))
        self._changed = True

    def delete_files(self, digests):
        self.conn.executemany('delete from names where digest=?',
                              ((d,) for d in digests))
        self.conn.executemany('delete from files where digest=?',
                              ((d,) for d in digests))
        self._changed = True

    def set_names(self, digest, names):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        self.conn.execute('delete from names where digest=?', (digest,))
        self.conn.executemany('insert into names (digest, name)'
                              ' values (?, ?)',
                              [(digest, name) for name in names])
        self._changed = True

    def set_names_batch(self, files, digests):
        for digest in digests:
            assert len(digest) == util.SHA256_LEN, repr(digest)
        self.conn.executemany('delete from names where digest=?',
                              ((d,) for d in digests))
        self.conn.executemany('insert into names (name, digest)'
                              ' values (?, ?)', files.items())
        self._changed = True

    def list_files(self):
        """Generate list of all objects in the repo.  Generates hash key
        for each object.
        """
        c = self.conn.cursor()
        c.execute('select digest from files'
                  ' order by digest')
        return (digest for (digest,) in c.fetchall())

    def list_file_names(self):
        """Generate list of all files in the repo.  Generates hash key
        and meta data dictionary pairs.
        """
        c = self.conn.cursor()
        c.execute('select name, digest from names'
                  ' order by name')
        return c.fetchall()

    def get_deleted(self):
        c = self.conn.cursor()
        c.execute('select digest from files'
                  ' where digest not in (select digest from names)')
        return (digest for (digest,) in c.fetchall())

    def get_sizes(self):
        c = self.conn.cursor()
        c.execute('select names.name, files.digest, files.size'
                  ' from files'
                  ' inner join names'
                  ' where files.digest=names.digest')
        return c.fetchall()

    def _copy_tmp(self, src_fn):
        # copy external file to temporary file inside repo, store hash
        if not DRY_RUN:
            ensure_dir(self.tmp_dir)
            tmp = tempfile.NamedTemporaryFile(dir=self.tmp_dir)
        else:
            tmp = None
        digest, tmp = util.hash_file(src_fn, tmp=tmp)
        if not DRY_RUN:
            mtime = os.stat(src_fn).st_mtime
            util.set_xattr_hash(tmp.name, digest, mtime)
        return digest, tmp


    def _link_in(self, fn, digest):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        key_abs = self.data(digest)
        if not DRY_RUN:
            util.set_xattr_hash(fn, digest)
            if not os.path.exists(key_abs):
                ensure_dir(os.path.dirname(key_abs))
            else:
                os.unlink(key_abs)
            os.link(fn, key_abs)
            os.chmod(key_abs, 0o422)

    def copy_in(self, src_fn, name):
        """Copy external file into repo (different filesystem)"""
        st = os.stat(src_fn)
        digest = self.find_name_size(name, st.st_size)
        if digest:
            log('file with same name and size exists, skipping %s' % src_fn)
            return digest
        digest, tmp = self._copy_tmp(src_fn)
        if not self.exists(digest):
            log('copy new %s' % src_fn)
            self._link_in(tmp.name, digest)
            self.add_file(digest, tmp.name)
            self.add_name(digest, name)
        else:
            if name not in self.get_names(digest):
                if not NO_ALIAS:
                    log('file exists, add name %s' % name)
                    self.add_name(digest, name)
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
        if not DRY_RUN:
            ensure_dir(os.path.dirname(dst_fn))
            if os.path.exists(dst_fn):
                log('skip existing %s' % dst_fn)
            else:
                os.link(key_abs, dst_fn)


    def link_overwrite(self, digest, dst_fn):
        """Link repo object over existing file"""
        key_abs = self.data(digest)
        if os.path.samefile(key_abs, dst_fn):
            log('linked already, skipping %s' % dst_fn)
        else:
            print('link over %s' % dst_fn)
            if not DRY_RUN:
                with util.write_access(os.path.dirname(dst_fn)):
                    util.link_over(key_abs, dst_fn)


def parse_index(fn):
    index = {}
    with util.open_text(fn) as fp:
        for line in fp:
            name, _, digest = line.rstrip().rpartition(' ')
            index[name] = digest
    return index



def do_import(args, repo, prefix):
    def store(src_fn):
        name = os.path.join(prefix, src_fn).strip('/')
        size = os.stat(src_fn).st_size
        digest = repo.find_name_size(name, size)
        if digest:
            log('file with same name and size exists, skipping %s' % src_fn)
            return
        digest = attr_digest = util.get_xattr_hash(src_fn)
        if digest is None:
            log('computing digest', src_fn)
            digest, tmp = util.hash_file(src_fn)
            assert len(digest) == util.SHA256_LEN, repr(digest)
        if repo.exists(digest):
            key_abs = repo.data(digest)
            if not os.path.samefile(src_fn, key_abs):
                print('link from repo %r' % src_fn)
                if not DRY_RUN:
                    util.link_over(key_abs, src_fn)
            else:
                print('skip existing %r' % src_fn)
        else:
            print('import', src_fn)
            repo.link_in(src_fn, digest)
            repo.add_file(digest, src_fn)
        # save filename in meta data
        repo.add_name(digest, name)

    t = time.time()
    for fn in _walk_files(args):
        if os.path.isfile(fn):
            store(fn)
        else:
            print('skip non-file', fn)
        if time.time() - t > 5:
            log('committing changes')
            repo.commit()
            t = time.time()
    print('done.')


def do_copy(args, repo, prefix):
    t = time.time()
    for fn in _walk_files(args):
        if os.path.isfile(fn):
            name = os.path.join(prefix, fn).strip('/')
            digest = repo.copy_in(fn, name)
        else:
            print('skip non-file', fn)
        if time.time() - t > 5:
            log('committing changes')
            repo.commit()
            t = time.time()
    repo.commit()
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


def _build_index(repo):
    index = {}
    for name, digest in repo.list_file_names():
        i = 1
        name = util.clean_name(name)
        path = name
        while path in index:
            path = '%s.%d' % (name, i)
            i += 1
        log(path, digest)
        index[path] = digest
    return index


def do_index_db(repo):
    fn = os.path.join(repo.root, 'index.db')
    if os.path.exists(fn):
        raise SystemExit('index db exists')
    def gen_files():
        for name, digest in repo.list_file_names():
            yield (digest, data.get('size'), data.get('mtime'))
    def gen_names():
        for name, digest in repo.list_file_names():
            for name in data['names']:
                yield (digest, name)
    with sql.connect(fn) as conn:
        conn.executescript(_DB_SCHEMA)
        conn.executemany('insert into files (digest, size, mtime)'
                         ' values (?, ?, ?)', gen_files())
        conn.executemany('insert into names (digest, name)'
                         ' values (?, ?)', gen_names())
        conn.commit()

def do_index(repo):
    index = _build_index(repo)
    out_fn = os.path.join(repo.root, 'index.txt')
    fp = tempfile.NamedTemporaryFile('w', dir=repo.root, prefix='tmp-index.',
                                     delete=False)
    for path in sorted(index):
        fp.write('%s %s\n' % (path, index[path]))
    print('created index', out_fn)
    os.rename(fp.name, out_fn)
    fp.close()

def do_fix_path(repo):
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
        print('set_names')
        repo.set_names_batch(filenames, digests)
        repo.commit()



def do_scrub(repo):
    global CONTINUE
    n = 0
    err_file = os.path.join(repo.root, 'errors.txt')
    with util.open_text(err_file, 'a', buffering=1) as err_fp:
        def err(*args):
            print(*args, file=err_fp)
        err('scrub started %s' % datetime.datetime.now())
        if CONTINUE:
            err('continue from %s' % CONTINUE)
        for digest in repo.list_files():
            if CONTINUE:
                if digest == CONTINUE:
                    CONTINUE = False
                else:
                    continue # skip
            fn = repo.data(digest)
            if not os.path.exists(fn):
                err('missing data', digest)
                continue
            digest2, tmp = util.hash_file(fn)
            log(digest, digest2)
            if digest != digest2:
                err('checksum mismatch', digest)
                n += 1
            else:
                digest2 = util.get_xattr_hash(fn)
                if digest2 != digest:
                    err('update xattr', digest)
                    util.set_xattr_hash(fn, digest)
                    n += 1
    if n:
        print('problems were found (%s), see errors.txt' % n)


def do_link_files(args, repo):
    import fnmatch
    index = {}
    for name, digest in repo.list_file_names():
        index[name] = digest
    log('loaded %d files' % len(index))
    for pat in args:
        for fn in index:
            if fnmatch.fnmatch(fn, pat):
                print('link', fn)
                if not DRY_RUN:
                    ensure_dir(os.path.dirname(fn))
                repo.link_to(index[fn], fn)


def do_list_files(args, repo):
    import fnmatch
    index = {}
    for name, digest in repo.list_file_names():
        for pat in args:
            if fnmatch.fnmatch(name.lower(), pat):
                print(name, digest)


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


def do_write_names(args, repo):
    if len(args) != 1:
        raise SystemExit('need one argument, index file')
    index = parse_index(args[0])
    digests = set()
    objects = {}
    for name, digest in index.items():
        digests.add(digest)
        objects.setdefault(digest, []).append(name)
    for digest in digests:
        m = repo.get_meta(digest)
        if not m:
            filename = repo.data(digest)
            repo.add_file(digest, filename)
    for digest, names in objects.items():
        repo.set_names(digest, names)


def do_show_deleted(repo):
    for digest in repo.get_deleted():
        print(digest)

def do_delete_missing(args, repo):
    for digest in args:
        assert not repo.exists(digest)
    repo.delete_files(args)

def do_delete(args, repo):
    digests = args
    repo.delete_files(digests)


def main():
    global DRY_RUN, LINK, FORCE, CONTINUE, NO_ALIAS

    import optparse
    import logging
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--repo', '-r', default=None)
    parser.add_option('--prefix', '-p', default='/')
    parser.add_option('--dst', '-d', default=None)
    parser.add_option('--continue', '-c', default=None, dest='cont')
    parser.add_option('--no-alias', '-A', default=False,
                      dest='no_alias', action="store_true")
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
    if len(args) == 0:
        raise SystemExit(USAGE)
    action = args[0]
    args = args[1:]
    level = logging.WARNING # default
    NO_ALIAS = options.no_alias
    if options.verbose == 1:
        level = logging.INFO
    elif options.verbose > 1:
        level = logging.DEBUG
    DRY_RUN = options.dryrun
    util.VERBOSE = options.verbose
    FORCE = options.force
    CONTINUE = options.cont
    #LINK = options.link
    logging.basicConfig(level=level)
    if not options.repo:
        raise SystemExit('need --repo option')
    os.umask(0o002)
    repo = Repo(options.repo)
    if action == 'init':
        repo.init()
    elif action == 'import':
        # compute content-ids, link into repo, write meta-data
        do_import(args, repo, options.prefix)
    elif action == 'copy':
        # copy files from another device, then import
        do_copy(args, repo, options.prefix)
    elif action == 'index':
        do_index(repo)
    elif action == 'fix-path':
        do_fix_path(repo)
    elif action == 'scrub':
        # check hashes
        do_scrub(repo)
    elif action == 'annex-fix':
        # crawl symlinks, link found objects into .git/annex/objects
        annex_fix(args, repo)
    elif action == 'annex-add':
        # crawl files, add to annex objects, replace file with symlink
        annex_add(args, repo)
    elif action == 'link':
        # link files matching regex patterns
        do_link_files(args, repo)
    elif action == 'write-names':
        # load names from index.txt or index.txt.new
        do_write_names(args, repo)
    elif action == 'deleted':
        do_show_deleted(repo)
    elif action == 'ls':
        do_list_files(args, repo)
    elif action == 'delete-missing':
        do_delete_missing(args, repo)
    elif action == 'delete':
        do_delete(args, repo)
    elif action == 'du-save':
        do_du_save(args, repo)
    else:
        raise SystemExit('unknown action %r' % action)
    repo.commit()

if __name__ == '__main__':
    main()
