#-*- coding:utf-8 -*-

import os
import email
import email.header
import datetime
import time
import logging
import argparse
import ConfigParser
import sys
from imapclient import IMAPClient
from multiprocessing.pool import ThreadPool

logger = logging.getLogger('mailer')
logger.setLevel(logging.DEBUG)

#set to stdout
ch = logging.FileHandler('mailer.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
#for windows hidden attr
FILE_ATTRIBUTE_HIDDEN = 0x02
PATH_SPECIAL_CHARS = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']


def _fix_name(filename):
    fix = filename
    for s in PATH_SPECIAL_CHARS:
        fix = fix.replace(s, '-')
    return fix


class Mailer():
    def __init__(self,
                 root=None,
                 imap=None,
                 ssl=False,
                 groups=None,
                 poolsize=0x0a,
                 pagesize=0x64,
                 **users):
        self.root = root
        self.imap = imap
        self.users = users
        self.ssl = ssl
        self.host = None
        self.port = 143
        self.cache = None
        self.groups = groups
        self.pagesize = pagesize
        #the max size is 10
        self.tp = ThreadPool(poolsize)

    def _list_all_clients(self):
        hosts = self.imap.split(':')
        self.host = hosts[0]
        if len(hosts) >= 2:
            self.port = hosts[1]
        cache = {}
        for u, p in self.users.items():
            try:
                conn = IMAPClient(
                    self.host, port=self.port, ssl=self.ssl, use_uid=True)
                conn.login(u, p)
                cache[u] = conn
            except Exception as e:
                logger.error('create imap client failed %s', e)
                raise IOError(e.message)
        #return all clients
        return cache

    def _list_mailbox(self, conn):
        mailboxes = []
        folders = conn.list_folders()
        for (_, _, name) in folders:
            mailboxes.append(name)
        return mailboxes

    def _download(self, user, name, conn):
        conn.select_folder(name)
        messages = conn.search()
        #split for many segment
        _key = u'{0}-{1}'.format(user, name)
        _history = self.cache.get(_key, None)
        _download_list = messages
        index = 0
        if _history is None:
            #download list
            self.cache[_key] = set()
            _download_list = list(_download_list)
            _download_list.reverse()
        #index +1 is len
        else:
            _download_list = list(set(messages).difference(set(_history)))
            _download_list.reverse()
        if len(_download_list) <= 0:
            #nothing to do
            logger.warn('Email(%s[%s]) NO NEW MESSAGE TO BE RECEIVED!!', user,
                        name)
            return
        print('\n' + self._get_dir(user) + '/' + name + ':')
        sys.stdout.write("\r%d/%d" % (index, len(_download_list)))
        sys.stdout.flush()
        if len(_download_list) > self.pagesize:
            while len(_download_list) > index:
                end = index + self.pagesize
                old = index
                if len(_download_list) < end:
                    end = len(_download_list)
                #unmark read message
                response = conn.fetch(_download_list[index:end],
                                      ['BODY.PEEK[]'])
                index = end
                for msg_id, data in response.items():
                    old = old + 1
                    sys.stdout.write("\r%d/%d" % (old, len(_download_list)))
                    sys.stdout.flush()
                    self._handle(user, name, msg_id, data)
                    time.sleep(0.1)
                self.cache[_key].update(_download_list[0:end])
                self._flush_meta(key=_key)
        else:
            response = conn.fetch(_download_list, ['BODY.PEEK[]'])
            for msg_id, data in response.items():
                index = index + 1
                sys.stdout.write("\r%d/%d" % (index, len(_download_list)))
                sys.stdout.flush()
                self._handle(user, name, msg_id, data)
                time.sleep(0.1)
            self.cache[_key].update(_download_list)
            self._flush_meta(key=_key)
        sys.stdout.write("\n")
        #conn.unselect_folder()

    def _handle(self, user, name, msg_id, data):
        #RFC822
        msg = email.message_from_string(data['BODY[]'])
        text, encoding = email.header.decode_header(msg['Subject'])[0]
        if encoding is None:
            encoding = 'gb2312'
        if msg.get('date') is None or msg.get('date').strip() == '':
            return
        _date = datetime.datetime.fromtimestamp(
            time.mktime(email.utils.parsedate(msg.get('date'))))
        self._save(user, name, _date, msg_id, self._try_decode(text, encoding),
                   data['BODY[]'])

    def _try_decode(self, name, encoding):
        try:
            return name.decode(encoding)
        except Exception as e:
            return 'InvalidEncodingFile'

    def download(self):
        self.cache = self._load_meta()
        clients = self._list_all_clients()
        handlers = []
        for u, c in clients.items():
            #self._wrap_download(c,u)
            handlers.append(
                self.tp.apply_async(self._wrap_download, args=(c, u)))
        #wait for ok
        for h in handlers:
            h.get()
        self._flush_meta()

    def _wrap_download(self, c, u):
        boxes = self._list_mailbox(c)
        for b in boxes:
            self._download(u, b, c)

    def _save(self, user, mbox, _date, uid, subject, data):
        self._mkdir(self.root, mode=0777)
        p = u'{0}/email/{1}/{2}/{3}'.format(self.root, self._get_dir(user),
                                            mbox, _date.strftime('%Y-%m-%d'))
        self._mkdir(p, mode=0777)
        eml = u'{0}/{1}-{2}.eml'.format(p, uid, _fix_name(subject))
        try:
            with open(eml, 'wb') as f:
                f.write(data)
                f.flush()
        except IOError as e:
            logger.error('write file failed %s', e.message)
            #save again
            self._save(user, mbox, _date, uid, 'InvalidFile', data)

    def _load_meta(self):
        mp = u'{0}/.meta'.format(self.root)
        self._mkdir(mp, mode=0777)
        cache = {}
        for f in os.listdir(mp):
            with open(mp + '/' + f, 'r') as ff:
                cache[f.replace('.meta', '')] = set([
                    int(x) if x != '' else -10000
                    for x in str(ff.read()).split(',')
                ])
        return cache

    def _flush_meta(self, key=None):
        mp = u'{0}/.meta'.format(self.root)
        self._mkdir(mp)
        self._hidden(mp)
        if key is not None:
            value = self.cache.get(key, None)
            if value is None:
                return
            with open(mp + '/' + key + '.meta', 'w+') as f:
                f.write(','.join(str(x) for x in value))
                f.flush()
        else:
            #flush all
            for k, v in self.cache.items():
                with open(mp + '/' + k + '.meta', 'w+') as f:
                    f.write(','.join(str(x) for x in v))
                    f.flush()

    def _hidden(self, path):
        if os.name == 'nt':
            import ctypes
            ret = ctypes.windll.kernel32.SetFileAttributesW(
                ur'{0}'.format(path), FILE_ATTRIBUTE_HIDDEN)
            if ret:
                logger.debug('hidden success')
        #do nothing
        else:
            logger.debug('os is unix like system, do nothing')

    def _mkdir(self, path, mode=0777):
        if os.path.exists(path):
            return
        os.makedirs(path, mode)

    def _get_dir(self, user):
        if self.groups is None:
            return user
        for g, emails in self.groups.items():
            for e in emails:
                if e == user:
                    return '{0}/{1}'.format(g, user)
        return user


def parser(*args):
    p = argparse.ArgumentParser(description='simple command line parser')
    #two args
    p.add_argument('-c', '--config', help='special config file')
    p.add_argument('-p', '--data', help='where data to store')
    return p.parse_args(args)


def parser_ini(cfg):
    cp = ConfigParser.ConfigParser()
    cp.readfp(open(cfg, 'r'))
    return cp


if __name__ == '__main__':
    p = parser(*sys.argv[1:])
    cp = p.config
    dp = p.data
    if p.config is None:
        logger.warn('config file is None,use default file config.ini')
        cp = os.path.join(os.getcwd(), 'config.ini')
    if p.data is None:
        logger.warn('data path is None, use default path is ' + os.getcwd())
        dp = os.getcwd()
    if not os.path.exists(cp) or not os.path.exists(dp):
        logger.error('config file or data path is not exist')
        os._exit(1)
    ini = parser_ini(cp)
    users = dict()
    for item in ini.get('mailer', 'users').split(','):
        arr = item.split(':')
        if len(arr) < 2:
            continue
        # if share mode
        if '(' in arr[0] and ')' in arr[0]:
            uu = arr[0].split('(')[1].split(')')[0].split('|')
            for u in uu:
                users[u] = arr[1]
        else:
            #if standard
            users[arr[0]] = arr[1]

    mapping = dict()
    groups = ini.options('group')
    for g in groups:
        mapping[g] = ini.get('group', g).split(',')
    imap = ini.get('mailer', 'imap')
    ssl = ini.getboolean('mailer', 'ssl')
    pagesize = 0x64 if ini.getint('mailer', 'pagesize') == 0 else ini.getint(
        'mailer', 'pagesize')
    poolsize = 0x0a if ini.getint('mailer', 'poolsize') == 0 else ini.getint(
        'mailer', 'poolsize')
    m = Mailer(
        root=dp,
        imap=imap,
        ssl=ssl,
        groups=mapping,
        pagesize=pagesize,
        poolsize=poolsize,
        **users)
    m.download()