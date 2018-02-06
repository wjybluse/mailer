#-*- coding:utf-8 -*-
from imapclient import IMAPClient
import os
import email
import email.header
import datetime
import time
import logging
import argparse
import ConfigParser
import sys

logger = logging.getLogger('mailer')
logger.setLevel(logging.DEBUG)

#set to stdout
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class Mailer():
    def __init__(self, root=None, imap=None, ssl=False, **users):
        self.root = root
        self.imap = imap
        self.users = users
        self.ssl = ssl
        self.host = None
        self.port = 143
        self.cache = None

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
                return cache
            except Exception as e:
                logger.error('create imap client failed %s', e.message)
                raise 'Connection error ' + e.message

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
        start = self.cache.get(u'{0}-{1}'.format(user, name), None)
        index = 0
        ii = 0
        if start is None:
            start = 0
        else:
            ii = self._get_index(start, messages)
        if len(messages[ii:]) > 100:
            messages = messages[ii:]
            while len(messages) > index:
                end = index + 100
                if len(messages) < end:
                    end = len(messages)
                response = conn.fetch(messages[index:end], ['RFC822'])
                index = end
                for msg_id, data in response.items():
                    self._handle(user, name, msg_id, data)
        else:
            response = conn.fetch(messages[ii:], ['RFC822'])
            for msg_id, data in response.items():
                self._handle(user, name, msg_id, data)
        self.cache[u'{0}-{1}'.format(user, name)] = messages[-1]
        conn.unselect_folder()

    def _get_index(self, start, messages):
        for index in range(0, messages):
            if messages[index] == start:
                return index
            if messages[index] > start:
                return index - 1
        return 0

    def _handle(self, user, name, msg_id, data):
        msg = email.message_from_string(data['RFC822'])
        text, encoding = email.header.decode_header(msg['Subject'])[0]
        if encoding is None:
            encoding = 'gb2312'
        if msg.get('date') is None or msg.get('date').strip() == '':
            return
        _date = datetime.datetime.fromtimestamp(
            time.mktime(email.utils.parsedate(msg.get('date'))))
        self._save(user, name, _date, msg_id, text.decode(encoding),
                   data['RFC822'])

    def download(self):
        self.cache = self._load_meta()
        clients = self._list_all_clients()
        tasks = []
        for u, c in clients.items():
            boxes = self._list_mailbox(c)
            for b in boxes:
                self._download(u, b, c)
        #wait for ok
        self._flush_meta()

    def _save(self, user, mbox, _date, uid, subject, data):
        self._mkdir(self.root, mode=755)
        p = u'{0}/email/{1}/{2}/{3}'.format(self.root, user, mbox,
                                            _date.strftime('%y-%m-%d'))
        self._mkdir(p, mode=755)
        eml = u'{0}/{1}-{2}.eml'.format(p, uid, subject)
        try:
            with open(eml, 'wb') as f:
                f.write(data)
                f.flush()
                f.close()
        except IOError as e:
            logger.error('write file failed %s', e.message)
            #save again
            self._save(user, mbox, _date, uid, 'SubjectWithInvalidCharacter',
                       data)

    def _load_meta(self):
        mp = u'{0}/.meta'.format(self.root)
        self._mkdir(mp, mode=755)
        cache = {}
        for f in os.listdir(mp):
            with open(mp + '/' + f, 'r') as ff:
                for line in ff.readlines():
                    splits = line.split(':')
                    if len(splits) < 2:
                        continue
                    cache[u'{0}-{1}'.format(f.replace('.meta', ''),
                                            splits[0])] = splits[1]
        return cache

    def _flush_meta(self):
        mp = u'{0}/.meta'.format(self.root)
        self._mkdir(mp)
        tmp = {}
        for key, value in self.cache.items():
            splits = key.split('-')
            if not tmp.get(splits[0], None):
                tmp[splits[0]] = set()
            tmp[splits[0]].add('{0}:{1}'.format(splits[0], value))
        for k, v in tmp.items():
            with open(mp + '/' + k + '.meta', 'wb') as f:
                f.write("\n".join(v))
                f.flush()
                f.close()

    def _mkdir(self, path, mode=755):
        if os.path.exists(path):
            return
        os.makedirs(path, mode)


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
    if p.config is None:
        logger.error('config file is None')
        exit(1)
    if p.data is None:
        logger.error('data path is None')
        exit(1)
    if not os.path.exists(p.config) or not os.path.exists(p.data):
        logger.error('config file or data path is not exist')
        exit(1)
    ini = parser_ini(p.config)
    users = dict()
    for item in ini.get('mailer', 'users').split(','):
        arr = item.split(':')
        if len(arr) < 2:
            continue
        users[arr[0]] = arr[1]
    mapping = dict()
    for item in ini.get('mailer', 'groups').split(','):
        arr = item.split(':')
        if len(arr) < 2:
            continue
        mapping[arr[0]] = arr[1].split(';')
    imap = ini.get('mailer', 'imap')
    ssl = ini.getboolean('mailer', 'ssl')
    m = Mailer(root=p.data, imap=imap, ssl=ssl, **users)
    m.download()
