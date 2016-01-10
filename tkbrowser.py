#!/usr/bin/python3
# vim: set sts=4 sw=4 et ai:


import sys
import os
import traceback
from tkinter import *
import tkinter.messagebox
import tkinter.font
import tkinter.filedialog
import re
import collections
from io import StringIO
import repo

USAGE = "Usage: %prog [options]"
APP_TITLE = 'Repo Browser'
WHITE = '#ffffff'
GREY = '#eeeeee'
YELLOW = '#fff873'
FIXED_FONT = 'TkFixedFont'


class NewDialog(object):

    TITLE = 'New folder'

    def __init__(self, parent, *args, **kwargs):
        self.top = Toplevel(parent)
        self.top.title(self.TITLE)
        self.content(self.top, *args, **kwargs)
        self.top.focus()

    def content(self, top, *args, **kwargs):
        self.name = StringVar()
        f = Frame(top)
        f.pack()
        l = Label(f, text='Name')
        l.pack(side=LEFT)
        y = Entry(f, textvariable=self.name)
        y.pack(side=LEFT)
        y.focus()
        def close(event=None):
            top.destroy()
        def cancel(event=None):
            self.name.set('')
            close()
        b = Button(top, text='OK', command=close)
        b.pack(pady=5)
        top.bind('<Return>', close)
        top.bind('<Escape>', cancel)


class FileList(object):
    def __init__(self, master, tree, browser):
        self.master = master
        self.tree = tree
        self.browser = browser
        self._cwd = tree.root
        self._create_ui()

    def _create_ui(self):
        f = Frame(self.master)
        f.pack(side=LEFT, expand=1, fill=Y)

        self._cur_path = StringVar()
        pl = Label(f, textvariable=self._cur_path)
        pl.pack()

        self.lb = Listbox(f, width=50, font=FIXED_FONT, selectmode=EXTENDED)
        self.lb.pack(side=LEFT, expand=1, fill=Y)
        self.lb.bind('<Double-Button-1>', self._list_select)

        s = Scrollbar(f, width=20)
        s.pack(side=LEFT, fill=Y)

        # hookup scrollbar
        self.lb.config(yscrollcommand=s.set)
        s.config(command=self.lb.yview)

        self._update()

        bind = self.lb.bind
        bind('<Control-x>', self.cut_items)
        bind('<Control-c>', self.copy_items)
        bind('<Control-v>', self.paste_items)
        bind('<Control-d>', self._delete_items)
        bind('<Control-n>', self._new_folder)

    def _get_sel(self):
        items = self.lb.curselection()
        if not items:
            return []
        return [self._files[int(i)] for i in items]

    def copy_items(self, event=None):
        self.tree.clip_items = self._get_sel()
        self.tree.clip_mode = 'copy'

    def _delete_items(self, event=None):
        self.tree.delete_items(self._get_sel())
        self.browser.update()

    def _new_folder(self, event=None):
        d = NewDialog(self.master)
        self.master.wait_window(d.top)
        v = d.name.get() or ''
        self._cwd.get_child(v)
        self.browser.update()

    def cut_items(self, event=None):
        self.tree.clip_items = self._get_sel()
        self.tree.clip_mode = 'move'

    def paste_items(self, event=None):
        self.tree.paste_items(self._cwd)
        self.browser.update()

    def _list_select(self, event):
        w = event.widget
        i = int(w.curselection()[0])
        n = self._files[i]
        self._cwd = n
        self._update()

    def _update(self):
        self.lb.delete(0, END)
        n = self._cwd
        self._cur_path.set(n.get_path())
        self._files = []
        if n.parent is not None:
            self.lb.insert(END, '..')
            self._files.append(n.parent)
        for name, child in sorted(n.children.items()):
            text = name
            if child.key is None:
                text += '/'
            self.lb.insert(END, text)
            self._files.append(child)



class Node(object):
    def __init__(self, name, parent):
        self.name = name
        self.parent = parent
        self.key = None
        self.children = {}

    def get_child(self, name):
        if name in self.children:
            return self.children[name]
        node = self.children[name] = Node(name, self)
        return node

    def get_path(self):
        parts = []
        node = self
        while node.parent is not None:
            parts.append(node.name)
            node = node.parent
        parts.reverse()
        return '/'.join(parts)

    def __contains__(self, name):
        return name in self.children

    def __setitem__(self, name, node):
        self.children[name] = node

    def __getitem__(self,name):
        return self.children[name]

    def remove(self, node):
        del self.children[node.name]



class Tree(object):
    def __init__(self, repo):
        self.repo = repo
        self.root = Node('', None)
        self.clip_items = []
        self.clip_mode = None
        self._load()

    def get_node(self, path):
        obj = self.root
        for part in path.split('/'):
            obj = obj.get_child(part)
        return obj

    def _load(self):
        fn = os.path.join(self.repo.root, 'index.txt.new')
        if os.path.exists(fn):
            for digest, name in self.repo.parse_index(fn):
                node = self.get_node(name)
                assert node.key is None, (node.key, node.get_path())
                node.key = digest
        else:
            for digest, meta in self.repo.list_files():
                for name in meta['names']:
                    node = self.get_node(name)
                    assert node.key is None, (node.key, node.get_path())
                    node.key = digest

    def delete_items(self, nodes):
        for n in nodes:
            print('rm %s' % n.name)
            n.parent.remove(n)

    def paste_items(self, node):
        if self.clip_mode != 'move':
            print('skip operation', self.clip_mode)
            return
        for n in self.clip_items:
            if n.name in node:
                if n.key is None:
                    continue
                other = node.children[n.name]
                if n.key != other.key:
                    print('collision %s %s %s', (n.name, n.key, other.key))
                    continue
                else:
                    print('discard %s' % n.name)
                    n.parent.remove(n)
            else:
                node[n.name] = n
                n.parent.remove(n)
                n.parent = node
        self.clip_items = []


class Browser(object):
    def __init__(self, master, tree):
        self.master = master
        self.tree = tree
        self._create_ui()
        self._update_title()

    def _update_title(self):
        title = APP_TITLE
        self.master.title(title)

    def update(self):
        self.l1._update()
        self.l2._update()

    def close(self, event=None):
        self.master.destroy()

    def save_file(self, event=None):
        index = {}
        todo = collections.deque([self.tree.root])
        while todo:
            n = todo.popleft()
            if n.key:
                if n.key not in index:
                    index[n.key] = []
                index[n.key].append(n.get_path())
            todo.extend(n.children.values())
        lines = []
        for digest, names in index.items():
            for name in names:
                lines.append('%s %s\n' % (name, digest))
        lines.sort()
        fn = os.path.join(self.tree.repo.root, 'index.txt.new')
        with repo.util.open_text(fn, 'w') as fp:
            fp.writelines(lines)

    def _create_ui(self):
        f = Frame(self.master, height=900)
        f.pack(expand=1, fill=Y)

        self.l1 = FileList(f, self.tree, self)
        self.l2 = FileList(f, self.tree, self)

        f = Frame(self.master)
        f.pack()
        self._save_button = Button(f, text='Save', command=self.save_file)
        self._save_button.pack(side=LEFT)
        Button(f, text='Quit', command=self.close).pack(side=LEFT)

        bind = self.master.bind
        bind('<Control-q>', self.close)
        bind('<Control-s>', self.save_file)


def main():
    global DEBUG
    import optparse
    parser = optparse.OptionParser(USAGE)
    parser.add_option('--debug', '-d', action='store_true')
    parser.add_option('--repo', '-r', default=None)
    options, args = parser.parse_args()
    DEBUG = options.debug
    filename = options.repo
    if not filename:
        raise SystemExit('filename not specified')

    r = repo.Repo(options.repo)
    tree = Tree(r)
    top = Tk()
    top.title(APP_TITLE)
    top.wm_geometry('+0+0')
    top.withdraw()
    default_font = tkinter.font.nametofont("TkDefaultFont")
    default_font.configure(size=11)

    b = Browser(top, tree)
    #b.lb.focus()
    top.deiconify()
    mainloop()

if __name__ == '__main__':
    main()
