#!/usr/bin/python3
# vim: set ai tw=74 sts=4 sw=4 et:
#
# Content indexed file repository

USAGE = "Usage: %prog [options]"

import os
import tempfile
import collections
import fcntl
import json
import util
from util import log
import re

DRY_RUN = False
LINK = False
FORCE = False


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
            with util.open_text(meta, 'r') as fp:
                data = json.load(fp)
        fcntl.flock(key, fcntl.LOCK_UN)
    return data


class Repo(object):
    def __init__(self, fn):
        self.root = fn
        self.tmp_dir = os.path.join(fn, 'tmp')
        self.obj_abs = os.path.join(fn, 'objects')

    def key(self, digest):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        return 'SHA256/%s/%s/%s' % (digest[:3], digest[3:6], digest[6:])

    def data(self, digest):
        return os.path.join(self.obj_abs, self.key(digest) + '.d')

    def meta(self, digest):
        return os.path.join(self.obj_abs, self.key(digest) + '.m')

    def filename_digest(self, fn):
        fn = fn.strip()[:-2]
        parts = fn.split(os.path.sep)
        return ''.join(parts[-3:])

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
            util.set_xattr_hash(tmp.name, digest, mtime)
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
        assert len(digest) == util.SHA256_LEN, repr(digest)
        key_abs = self.data(digest)
        if os.path.exists(key_abs):
            return
        if not DRY_RUN:
            ensure_dir(os.path.dirname(key_abs))
            os.link(fn, key_abs)
            util.set_xattr_hash(key_abs, digest)
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
                with util.write_access(os.path.dirname(dst_fn)):
                    util.link_over(key_abs, dst_fn)


    def read_meta(self, digest):
        key_abs = self.data(digest)
        return _read_meta(key_abs)


    def write_meta(self, digest, name):
        assert len(digest) == util.SHA256_LEN, repr(digest)
        if DRY_RUN:
            return
        key_abs = self.data(digest)
        with open(key_abs, 'rb') as key:
            fcntl.flock(key, fcntl.LOCK_EX)
            meta = key_abs.replace('.d', '.m')
            if os.path.exists(meta):
                with util.open_text(meta) as fp:
                    data = json.load(fp)
            else:
                data = {}
            old_data = data.copy()
            name = util.clean_name(name)
            names = data.get('names', [])
            if name not in names:
                names.append(name)
            data['names'] = names
            data['version'] = 1
            st = os.stat(key_abs)
            data['size'] = st.st_size
            mtime = int(st.st_mtime)
            if 'mtime' not in data or data['mtime'] > mtime:
                data['mtime'] = mtime
            if data != old_data:
                with util.open_text(meta + '.new', 'w') as fp:
                    log('write meta %s' % data)
                    json.dump(data, fp, indent=4)
                os.rename(meta + '.new', meta)
            fcntl.flock(key, fcntl.LOCK_UN)


    def parse_index(self):
        index_fn = os.path.join(self.root, 'index.txt')
        with util.open_text(index_fn) as fp:
            for line in fp:
                name, _, digest = line.rstrip().rpartition(' ')
                yield name, digest


def do_import(args, repo, prefix):

    def store(src_fn):
        digest = attr_digest = util.get_xattr_hash(src_fn)
        if digest is None:
            log('computing digest', src_fn)
            digest, tmp = util.hash_file(src_fn)
            assert len(digest) == util.SHA256_LEN, repr(digest)
        if repo.exists(digest):
            key_abs = repo.data(digest)
            if not os.path.samefile(src_fn, key_abs):
                print('link over %r' % src_fn)
                if not DRY_RUN:
                    util.link_over(key_abs, src_fn)
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


def do_index(repo):
    index = {}
    for fn in _walk_files([repo.obj_abs]):
        if fn.endswith('.d'):
            data = _read_meta(fn)
            names = data.get('names') or []
            for name in names:
                i = 1
                name = util.clean_name(name)
                path = name
                while path in index:
                    path = '%s.%d' % (name, i)
                    i += 1
                digest = repo.filename_digest(fn)
                log(path, digest)
                index[path] = digest
    out_fn = os.path.join(repo.root, 'index.txt')
    with util.open_text(out_fn + '.new', 'w') as fp:
        for path in sorted(index):
            fp.write('%s %s\n' % (path, index[path]))
    os.rename(out_fn + '.new', out_fn)


def do_scrub(repo):
    sums = set()
    for name, digest in repo.parse_index():
        sums.add(digest)
    with util.open_text(os.path.join(repo.root, 'errors.txt'), 'w') as err:
        for digest in sorted(sums):
            fn = repo.data(digest)
            digest2, tmp = util.hash_file(fn)
            log(digest, digest2)
            if digest != digest2:
                mismatched.append(digest)
                print('checksum mismatch', digest, file=err)
            else:
                digest2 = util.get_xattr_hash(fn)
                if digest2 != digest:
                    print('update xattr', digest, file=err)
                    util.set_xattr_hash(fn, digest)


def do_link_files(args, repo):
    index = {}
    for name, digest in repo.parse_index():
        index[name] = digest
    for pat in args:
        p = re.compile(pat)
        for fn in index:
            if p.match(fn):
                print('link', fn)
                if not DRY_RUN:
                    ensure_dir(os.path.dirname(fn))
                repo.link_to(index[fn], fn)

def main():
    global DRY_RUN, LINK, FORCE

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
    util.VERBOSE = options.verbose
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
    else:
        raise SystemExit('unknown action %r' % action)

if __name__ == '__main__':
    main()
