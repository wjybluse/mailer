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
import json
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


class Meta():
    def __init__(self, newest, segment=None):
        self.newest = newest
        self.segments = []
        if segment is not None:
            self.segments.append(segment)

    def add(self, segment):
        if segment is None:
            return
        self.segments.append(segment)

    def remove(self, segment):
        if segment is None:
            return
        self.segments.remove(segment)

    def list_segments(self):
        return self.segments

    def get_dict(self):
        return {'newest': self.newest, 'segments': self.segments}


class MetaEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        super(MetaEncoder, self).__init__(**kwargs)

    def default(self, obj):
        if isinstance(obj, Meta):
            return obj.get_dict()
        #call parent
        return super(MetaEncoder, self).default(obj)


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
                #if 163 imap server
                if self.host == 'imap.163.com':
                    conn.id_(
                        parameters={
                            'name': 'NeteaseFlashMail',
                            'version': '2.4.1.30',
                            'os': 'windows',
                            'os-version': '6.1.7601',
                            'vendor': 'NetEase,Inc.',
                            'support-url': 'mailclient@188.com'
                        })
                conn.login(u, p)
                cache[u] = conn
            except Exception as e:
                logger.error('create imap client failed %s', e.message)
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
        messages.reverse()
        #split for many segment
        meta = self.cache.get(u'{0}-{1}'.format(user, name), None)
        index = 0
        ii = 0
        start = 0
        if meta is None:
            start = 0
            meta = Meta(messages[0])
            #init
            self.cache[u'{0}-{1}'.format(user, name)] = meta
            #set to max
            ii = len(messages)
        else:
            ii = self._get_index(meta.newest, messages)
            #index +1 is len
            if ii == 0:
                #nothing to do
                logger.warn('Email(%s[%s]) NO NEW MESSAGE TO BE RECEIVED!!',
                            user, name)
                return
            if ii == -1:
                #read from newest?
                ii = 0
        print('\n' + self._get_dir(user) + '/' + name + ':')
        sys.stdout.write("\r%d/%d" % (index, len(messages[ii:])))
        sys.stdout.flush()
        if len(messages[0:ii]) > self.pagesize:
            _messages = messages[0:ii]
            old = None
            while len(_messages) > index:
                end = index + self.pagesize
                _old = index
                if len(_messages) < end:
                    end = len(_messages)
                #unmark read message
                response = conn.fetch(_messages[index:end], ['BODY.PEEK[]'])
                index = end
                for msg_id, data in response.items():
                    _old = _old + 1
                    sys.stdout.write("\r%d/%d" % (_old, len(_messages)))
                    sys.stdout.flush()
                    self._handle(user, name, msg_id, data)
                    time.sleep(0.1)
                self.cache[u'{0}-{1}'.format(user, name)].remove(old)
                if _messages[end] != _messages[-1]:
                    new_segment = (_messages[end], _messages[-1])
                    self.cache[u'{0}-{1}'.format(user, name)].add(new_segment)
                    old = new_segment
                self._flush_meta()
            #if has any segment do again
            for ss in meta.list_segments():
                begin = self._get_index(ss[0])
                end = self._get_index(ss[1])
                if begin - end <= self.pagesize:
                    _index = 0
                    rsp = conn.fetch(_messages[begin, end], ['BODY.PEEK[]'])
                    for msg_id, data in response.items():
                        sys.stdout.write("\r%d/%d" % (_index, begin - end))
                        _index += 1
                        sys.stdout.flush()
                        self._handle(user, name, msg_id, data)
                        time.sleep(0.1)
                    meta.remove(ss)
                else:
                    _index = 0
                    _olds = ss
                    while begin - end > _index:
                        if _index + self.pagesize > begin - end:
                            _end = self.pagesize
                        else:
                            _end = _index + self.pagesize
                        rsp = conn.fetch(_messages[begin, end],
                                         ['BODY.PEEK[]'])
                        for msg_id, data in response.items():
                            sys.stdout.write("\r%d/%d" % (_index, begin - end))
                            _index += 1
                            sys.stdout.flush()
                            self._handle(user, name, msg_id, data)
                            time.sleep(0.1)
                        _index = _end
                        self.cache[u'{0}-{1}'.format(user, name)].remove(_olds)
                        if _index < begin - end:
                            nn = (_end, end)
                            self.cache[u'{0}-{1}'.format(user, name)].add(nn)
                            _olds = nn
                        self._flush_meta()
        else:
            _messages = messages[ii:]
            response = conn.fetch(_messages, ['BODY.PEEK[]'])
            for msg_id, data in response.items():
                index = index + 1
                sys.stdout.write("\r%d/%d" % (index, len(_messages)))
                sys.stdout.flush()
                self._handle(user, name, msg_id, data)
                time.sleep(0.1)
            self.cache[u'{0}-{1}'.format(user, name)] = Meta(_messages[0])
            self._flush_meta()
        sys.stdout.write("\n")

        #conn.unselect_folder()
    def _get_index(self, start, messages):
        for index in range(0, len(messages)):
            if int(messages[index]) == int(start):
                return index
            if int(messages[index]) < int(start):
                #start from current index
                return index
        return -1

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
        self._save(user, name, _date, msg_id, text.decode(encoding),
                   data['BODY[]'])

    def download(self):
        self.cache = self._load_meta()
        clients = self._list_all_clients()
        handlers = []
        for u, c in clients.items():
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
        self._mkdir(self.root, mode=777)
        p = u'{0}/email/{1}/{2}/{3}'.format(self.root, self._get_dir(user),
                                            mbox, _date.strftime('%Y-%m-%d'))
        self._mkdir(p, mode=777)
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
        self._mkdir(mp, mode=777)
        cache = {}
        for f in os.listdir(mp):
            with open(mp + '/' + f, 'r') as ff:
                self.cache[f.replace('.meta', '')] = json.loads(
                    str(ff.read()), encoding='utf-8')
        return cache

    def _flush_meta(self):
        mp = u'{0}/.meta'.format(self.root)
        self._mkdir(mp)
        self._hidden(mp)
        for key, value in self.cache.items():
            with open(mp + '/' + key + '.meta', 'w+') as f:
                f.write(json.dumps(value, encoding='utf-8'))
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

    def _mkdir(self, path, mode=755):
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
