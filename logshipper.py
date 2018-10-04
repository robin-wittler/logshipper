#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'Robin Wittler'
__contact__ = 'r.wittler@mysportworld.de'
__license__ = 'GPL3+'
__copyright__ = '(c) 2013 by mysportworld.de'
__version__ = '0.2.2'

import os
import re
import pwd
import grp
import sys
import errno
import redis
import daemon
import signal
import logging
import threading
import pyinotify
import setproctitle
import dateutil.tz
import dateutil.parser
from glob import iglob
from time import sleep
from Queue import Queue
from pprint import pformat
from socket import getfqdn
from functools import wraps
from datetime import datetime
from socket import gethostname
from daemon import pidlockfile
from optparse import OptionParser
from ConfigParser import ParsingError
from ConfigParser import NoOptionError
from ConfigParser import NoSectionError
from ConfigParser import SafeConfigParser
from pyelasticsearch import ElasticSearch
from apscheduler.scheduler import Scheduler
from apscheduler.threadpool import ThreadPool
from logging.handlers import WatchedFileHandler


try:
    import simplejson as json
except ImportError:
    import json


BASE_FORMAT_SYSLOG = (
    '%(name)s.%(funcName)s[%(process)d] ' +
    '%(levelname)s: %(message)s'
)

BASE_FORMAT_STDOUT = '%(asctime)s ' + BASE_FORMAT_SYSLOG

logger = logging.getLogger(__name__)


class Error(Exception):
    pass


class ConfigError(Error):
    pass


class FilenameEndsWithDecorator(object):
    def __init__(self, ending):
        self.ending = ending

    def __call__(self, func):
        @wraps(func)
        def wrapped(*targs, **kwargs):
            if not targs[-1].name.endswith(self.ending):
                return None
            return func(*targs, **kwargs)
        return wrapped

filename_endswith = FilenameEndsWithDecorator


class ProcessConfigEvents(pyinotify.ProcessEvent):
    def my_init(self, callbacks=None):
        self.callbacks = callbacks

    @filename_endswith('.cfg')
    def process_default(self, event):
        logger.debug(
            'Received %r event for %r', event.maskname, event.pathname
        )

        if event.maskname == 'IN_CREATE' and not os.path.islink(event.pathname):
        # if a link is created in the watched dir, then there will be only
        # a IN_CREATE event. But a regular file, which will be copied, have
        # also a IN_CLOSE_WRITE event - and we wait for that event.
            logger.debug(
                'Received %r for regular file %r - ignoring it.',
                event.maskname,
                event.pathname
            )
            return None

        return self.call_callback(event)

    def call_callback(self, event):
        callback = self.callbacks.get(event.maskname, None)
        if callback is None:
            logger.debug(
                'No callback found for event_type %r',
                event.maskname
            )
        elif not callable(callback):
            logger.debug(
                'Callback %r for event_type %r is not callable.',
                callback,
                event.maskname
            )
        else:
            return callback(path=event.pathname)


class WatchManager(pyinotify.WatchManager):
    pass


class ThreadedNotifier(pyinotify.ThreadedNotifier):
    pass


class LogshipperScheduler(Scheduler):
    def __init__(self, **kwargs):
        super(LogshipperScheduler, self).__init__(**kwargs)
        self.jobs = dict()
        self.kwargs = kwargs

    def remove_job(self, job_path):
        job = self.jobs.pop(job_path, None)
        if job is None:
            logger.info(
                'Can not remove job %r - no such job found.',
                job_path
            )
            return False
        logger.info('Removing job %r from scheduler.', job)

        self.unschedule_job(job)
        return True

    def remove_all_jobs(self):
        for job_path, job in self.jobs.copy().iteritems():
            self.unschedule_job(job)
            self.jobs.pop(job_path)


    def add_new_job(self, func, logfile_config_path, read_interval):
        job = self.add_interval_job(
            func,
            seconds=read_interval,
            max_instances=1,
            misfire_grace_time=9999999999,
        )
        self.jobs.update({logfile_config_path: job})
        return job


class ConfigFile(SafeConfigParser):
    def __init__(self, path, **kwargs):
        SafeConfigParser.__init__(self, **kwargs)

        if isinstance(path, (list, tuple)):
            for p in path:
                self.check_file_existence(p)
        else:
            self.check_file_existence(path)


        self.path = path
        self.kwargs = kwargs

        logger.debug(
            'Reading config(s) at %r. Given kwargs are:\n\n%s\n',
            self.path,
            pformat(self.kwargs)
        )


    def __repr__(self):
        return (
            '<%s.%s(path=%r) instance at %s>'
            %(
                self.__class__.__module__,
                self.__class__.__name__,
                self.path, hex(id(self))
            )
        )

    def check_file_existence(self, path):
        if not os.path.exists(path):
            logger.debug('File %r does not exist.', path)
            raise IOError(
                errno.ENOENT,
                os.strerror(errno.ENOENT),
                path
            )
        logger.debug('File %r exist.', path)
        return True

    def validate_config(self):
        raise NotImplementedError('You must override this method!')

    def read(self, *targs, **kwargs):
        result = SafeConfigParser.read(self, self.path)
        if not result:
            raise ParsingError('Could not parse %r' %(self.path))

        try:
            self.validate_config()
        except Exception as error:
            logger.exception(error)
            raise ConfigError(
                'Validation of configfile %r failed with error: %s.'
                %(self.path, error)
            )


        logger.debug(
            'Parsed config at %r with resulting values:\n\n%s\n',
            self.path,
            pformat(
                [(section, self.items(section)) for section in self.sections()]
            )
        )


class LogshipperConfigFile(ConfigFile):
    def __init__(self, path, **kwargs):
        defaults = {
            'logdir': '/var/log/logshipper',
            'loglevel': 'error',
            'piddir': '/var/run/logshipper',
            'statusdir': '/var/cache/logshipper',
            'configdir': '/etc/logshipper/conf.d',
            'sender': 'redis',
            'sender_threads': '5',
            'patternsdir': '/etc/logshipper/patterns.d',
            'user': uid2username(os.geteuid()),
            'group': gid2groupname(os.getegid()),
            'max_messages': '1000',
            'read_interval': '3',

        }
        defaults.update(kwargs)

        ConfigFile.__init__(self, path, defaults=defaults)
        self.read()

    def validate_config(self):
        if not self.has_section('default'):
            raise NoSectionError('default')

        for name in self.defaults():
            if not self.has_option('default', name):
                raise NoOptionError(name, 'default')

        sender = self.get('default', 'sender')
        if not self.has_section(sender):
            raise NoSectionError(sender)

        loglevel = self.get('default', 'loglevel')
        if not hasattr(logging, loglevel.upper()):
            raise ValueError('Unknown loglevel: %r' %(loglevel))

    @property
    def sender(self):
        return self.get('default', 'sender')

    @property
    def sender_threads(self):
        return self.getint('default', 'sender_threads')

    @property
    def loglevel(self):
        return getattr(logging, self.get('default', 'loglevel').upper())

    @property
    def logdir(self):
        return self.get('default', 'logdir')

    @property
    def logfile(self):
        return os.path.join(self.logdir, 'logshipper.log')

    @property
    def piddir(self):
        return self.get('default', 'piddir')

    @property
    def pidfile(self):
        return os.path.join(self.piddir, 'logshipper.pid')

    @property
    def statusdir(self):
        return self.get('default', 'statusdir')

    @property
    def configdir(self):
        return self.get('default', 'configdir')

    @property
    def patterns_dir(self):
        return self.get('default', 'patternsdir')

    @property
    def user(self):
        return self.get('default', 'user')

    @property
    def group(self):
        return self.get('default', 'group')

    @property
    def max_messages(self):
        return self.getint('default', 'max_messages')

    @property
    def read_interval(self):
        return self.getint('default', 'read_interval')


class LogfileConfigFile(ConfigFile):
    _cpattern = None
    _patterns_invalid_names = ('fqdn', 'hostname', 'statusdir')

    def __init__(self, path, **kwargs):
        if not 'statusdir' in kwargs:
            raise ValueError(
                'You must give a argument named "statusdir".'
            )

        if not 'patterns' in kwargs:
            raise ValueError('You must give a argument named "patterns".')

        else:
            patterns = kwargs.pop('patterns')
            for name, pattern in patterns.copy().iteritems():
                if name in self._patterns_invalid_names:
                    raise ValueError(
                        'A pattern named %r is not allowed.' %(name)
                    )
                else:
                    patterns.update({name: pattern})

        defaults = {
            'fqdn': getfqdn(),
            'hostname': gethostname()
        }
        defaults.update(kwargs)
        defaults.update(patterns)

        ConfigFile.__init__(self, path, defaults=defaults)
        self.read()

    def validate_config(self):
        if not self.has_section('input'):
            raise NoSectionError('input')

        must_haves = ('file', 'type', 'tags', 'pattern')
        for key in must_haves:
            if not self.has_option('input', key):
                raise NoOptionError(key, 'input')

    @property
    def file(self):
        return self.get('input', 'file')

    @property
    def type(self):
        return self.get('input', 'type')

    @property
    def tags(self):
        return map(
            lambda x: x.lstrip().rstrip(),
            self.get('input', 'tags').split(',')
        )

    @property
    def pattern(self):
        return self.get('input', 'pattern')

    @property
    def cpattern(self):
        if self._cpattern is None:
            try:
                self._cpattern = re.compile(self.pattern)
            except Exception as error:
                logger.error(
                    (
                        'Could not compile pattern %r from file %r. ' +
                        'Error was: %s'
                    ),
                    self.pattern,
                    self.path,
                    error
                )
                raise
        return self._cpattern

    @property
    def status_file(self):
        status_dir = self.get('input', 'statusdir', None)
        if status_dir is None:
            return None
        else:
            return os.path.join(
                status_dir,
                '_'.join(self.file.split(os.path.sep))
            )

    @property
    def source(self):
        if self.has_option('input', 'fqdn'):
            return self.get('input', 'fqdn')

        if self.has_option('input', 'hostname'):
            return self.get('input', 'hostname')

    @property
    def timestamp_format(self):
        if self.has_option('input', 'timestamp_format'):
            return self.get('input', 'timestamp_format')
        else:
            return None


class PatternConfigFile(ConfigFile):
    def __init__(self, path, **kwargs):
        defaults = {}
        defaults.update(kwargs)

        ConfigFile.__init__(self, path, defaults=defaults)
        self.read()

    def validate_config(self):
        if not self.has_section('patterns'):
            raise NoSectionError('patterns')


class PatternsConfigDir(ConfigFile):
    def __init__(self, path, **kwargs):
        defaults = {}
        defaults.update(kwargs)

        files = iglob(os.path.join(path, '*.cfg'))

        ConfigFile.__init__(self, list(files), defaults=defaults)
        self.read()

    def validate_config(self):
        if not self.has_section('patterns'):
            raise NoSectionError('patterns')


class MessageSender(object):
    def __init__(self, *targs, **kwargs):
        super(MessageSender, self).__init__()
        self.targs = targs
        self.kwargs = kwargs
        logger = logging.getLogger(
            '%s.%s'
            %(os.path.basename(sys.argv[0]), self.__class__.__name__)
        )

    def send(self, *targs, **kwargs):
        raise NotImplementedError(
            'Please override send method.'
        )


class RedisSender(MessageSender):
    def __init__(self, *targs, **kwargs):
        super(RedisSender, self).__init__(*targs, **kwargs)
        self.host = self.kwargs.get('host')
        self.port = int(self.kwargs.get('port'))
        self.keyword = self.kwargs.get('keyword')
        self.last_send_time = 0

    def send(self, messages):

        redis_client = redis.Redis(
            host=self.host,
            port=self.port
        )

        logger.debug('Trying to send:\n\n%s\n', pformat(messages))

        messages = map(json.dumps, messages)

        # since python-redis 2.4.9, rpush is able
        # to handle lists directly
        if redis.VERSION >= (2, 4, 9):
            logger.debug(
                'Using a python redis version >= 2.4.9. ' +
                'Using the native list type.'
            )
            if redis_client.rpush(self.keyword, *messages):
                return True
        else:
            # but if we are using a version lower then
            # 2.4.9 we simply map :(
            logger.debug(
                'Using a python redis version < 2.4.9. ' +
                'Switching back to mapper fallback.'
            )
            if map(
                lambda x: redis_client.rpush(self.keyword, x),
                messages
            ):
                return True
        return False


class ElasticSearchSender(MessageSender):
    def __init__(self, *targs, **kwargs):
        super(ElasticSearchSender, self).__init__(*targs, **kwargs)
        self.host = self.kwargs.get('host')
        self.port = int(self.kwargs.get('port'))
        self.type = self.kwargs.get('type', '@type')

    def send(self, messages):
        if self.type == '@type':
            self.type = messages[0].get('@type')
            logger.debug(
                'Type is \'@type\' - setting it to %r',
                self.type
            )

        es = ElasticSearch(
            'http://%s:%s' %(self.host, self.port)
        )

        now = datetime.utcnow()
        index = now.strftime('logstash-%Y.%m.%d')

        result = es.bulk_index(index=index, doc_type=self.type, docs=messages)
        logger.debug(
            'Elasticsearch bulk_index run returned with:\n\n%s\n',
            pformat(result)
        )
        return True


supported_senders = {
    'redis': RedisSender,
    'elasticsearch': ElasticSearchSender,
}


class FileParser(object):
    def __init__(self, config, stop_event, event_sender_class,
                 event_sender_config, max_messages=1000, error_wait_time=300):
        self.config = config
        self.stop_event = stop_event
        self._event_sender_class = event_sender_class
        self._event_sender_config = event_sender_config
        self.event_sender = self._event_sender_class(
            **self._event_sender_config
        )
        self.max_messages = max_messages
        self.error_wait_time = error_wait_time

    def run(self):
        logger.debug('Startup!')
        with open(self.config.file, 'r', 1) as fp:
            actual_inode, actual_size = self.get_actual_inode_and_size(fp)
            logger.debug(
                'Opened %r and it has the actual inode %r and an ' +
                'actual size of %r',
                self.config.file,
                actual_inode,
                actual_size
            )
            status_data = self.read_status_file()
            logger.debug(
                'Status file %r has those values for file %r:\n' +
                pformat(status_data),
                self.config.status_file,
                self.config.file
            )
            is_rotated = self.is_rotated(
                actual_inode,
                actual_size,
                status_data
            )

            new_lines = self.file_have_new_lines(
                actual_size, status_data.get('last_position')
            )

            if is_rotated:
                start = 0
            else:
                if new_lines is False:
                    logger.debug('No new lines at %r', self.config.file)
                    return
                else:
                    logger.debug('%r have new lines', self.config.file)
                start = status_data.get('last_position')

            actual_position, messages = self.parse_file(fp, start)
            logger.debug(
                'parse_file() returned:\n\n' +
                'actual_position is: %r\nmessages is: %s\n',
                actual_position,
                pformat(messages)
            )

            if not messages:
                return

            while not self.stop_event.is_set():
                try:
                    self.call_event_sender(messages)
                except Exception as error:
                    logger.exception('Sender raises an error:\n%s', error)
                    logger.info(
                        'Waiting %s seconds before next retry ...',
                        self.error_wait_time
                    )
                    sleep(self.error_wait_time)
                    continue
                else:
                    logger.debug(
                        'Successfully deliver %r messages to sender %s',
                        len(messages),
                        self._event_sender_class
                    )

                    status_data = self.create_status_data(
                        actual_inode,
                        actual_position,
                    )

                    self.write_status_file(status_data)
                    return

    def parse_file(self, fp, start):

        messages = list()
        position = start
        last_message = None
        actual_lines = list()
        last_position = None

        for line in self.yield_lines(fp, start):
            if messages and (len(messages) == self.max_messages):
                return last_position, messages

            position += len(line)

            match = self.lineparser(line)

            if match is None:
                if last_message is None:
                    # found an unparsable message
                    logger.error(
                        'Line %r from file %r is an unparsable message. ' +
                        'Tagging line as unparsable.',
                        line,
                        self.config.file,
                    )
                    message = self.create_event_dict(
                        {'message': line},
                        unparsable=True,
                        multiline=False
                    )
                    messages.append(message)
                    last_message = None
                    last_position = position
                    continue
                else:
                    # found a multiline
                    logger.debug(
                        'Line %r from file %r is part of a multiline event.',
                        line,
                        self.config.file
                    )
                    actual_lines.append(line)
                    last_position = position
                    continue
            else:
                if actual_lines:
                    # found a new matching line.
                    # We assume that this is the end of a multiline.
                    logger.debug(
                        'Line %r from file %r ends previous multiline.',
                        line,
                        self.config.file
                    )
                    message = last_message.get('message')
                    message = '%s%s' %(
                        message,
                        ''.join(actual_lines)
                    )
                    last_message.update({'message': message})
                    last_position = (position - (len(line)))
                    actual_lines = list()
                    messages.append(
                        self.create_event_dict(last_message, multiline=True)
                    )

                elif last_message:
                    # found a new matching line, finalize the previous
                    # matching line
                    logger.debug(
                        'Line %r from file %r finalizes a previous match.',
                        line,
                        self.config.file
                    )
                    messages.append(
                        self.create_event_dict(last_message, multiline=False)
                    )
                    last_position = (
                        position - len(line)
                    )
                    actual_lines = list()

                last_message = match.groupdict()
        logger.debug(
            'End of File reached. No more lines to read.'
        )
        if last_message:
            multiline = False
            if actual_lines:
                message = last_message.get('message')
                message = '%s%s' %(
                    message,
                    ''.join(actual_lines)
                )
                last_message.update({'message': message})
                multiline = True

            messages.append(
                self.create_event_dict(last_message, multiline=multiline)
            )
            last_position = position
        return last_position, messages

    def file_have_new_lines(self, actual_size, last_size):
        return actual_size > last_size

    def get_actual_inode_and_size(self, fp):
        stats = os.fstat(fp.fileno())
        return stats.st_ino, stats.st_size

    def call_event_sender(self, messages):
        if self.event_sender.send(messages):
            logger.debug('Successfully sended %r messages.', len(messages))
            return True
        else:
            logger.debug(
                'Sending %r messages was not successfull.', len(messages)
            )
            raise RuntimeError(
                'Could not send %r messages with %r.',
                len(messages),
                self.event_sender
            )

    def is_rotated(self, actual_inode, actual_size, status_data):
        last_inode = status_data.get('last_inode')
        last_position = status_data.get('last_position')
        #last_line = status_data.get('last_line')

        if (actual_inode != last_inode):
            logger.info(
                'The actual inode differs from the last inode. ' +
                'Looks like %r was rotated.',
                self.config.file
            )
            return True
        elif (actual_size < last_position):
            logger.info(
                'The actual size differs from the last size. ' +
                'Looks like %r was rotated.',
                self.config.file
            )
            return True

    def read_status_file(self):
        logger.debug('Reading status file: %r', self.config.status_file)
        config = SafeConfigParser()
        if not config.read(self.config.status_file):
            logger.debug(
                'Could not read/parse status file at %r. ' +
                'Return default values.',
                self.config.status_file
            )
            config = dict()
        else:
            config = dict(config.items('status'))

        config.update({'last_inode': int(config.get('last_inode', 0))})
        config.update({'last_position': int(config.get('last_position', 0))})
        return config

    def write_status_file(self, data):
        logger.debug(
            'Writing data %r to status file %r',
            data,
            self.config.status_file
        )
        config = SafeConfigParser()
        config.add_section('status')
        for key, value in data.iteritems():
            config.set('status', key, str(value))
        with open(self.config.status_file, 'w', 1) as fp:
            return config.write(fp)

    def create_status_data(self, last_inode, last_position):
        return dict(
            last_inode=last_inode,
            last_position=last_position,
        )

    def yield_lines(self, fp, start):
        fp.seek(start, 0)
        for line in fp:
            yield line

    def lineparser(self, line):
        logger.debug('Parsing line: %r', line)
        return self.config.cpattern.match(line)

    def create_event_dict(self, message_dict, **extra_fields):
        fields = dict()

        fields.update(extra_fields)
        for key, value in message_dict.iteritems():
            if isinstance(value, (str, unicode)):
                value = value.decode('utf-8', 'ignore')
            fields.update({key: [value]})

        try:
            return {
                '@tags': self.config.tags,
                '@source_host': self.config.source,
                '@type': self.config.type,
                '@fields': fields,
                '@message': message_dict.get('message').decode(
                    'utf-8',
                    'ignore'
                ),
                '@source': 'file://%s%s' %(
                    self.config.source, self.config.file
                ),
                '@source_path': self.config.file,
                '@parsed_timestamp': datetime.utcnow().strftime(
                    '%Y-%m-%dT%H:%M:%S.000Z'
                ),
                '@timestamp': (
                    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                    if not 'timestamp' in fields else
                    self.format_timestamp_to_iso8601(
                        fields['timestamp'][0]
                    )
                )
            }

        except Exception as error:
            logger.error(
                'Can not create event dict for:\n\n%s\nError was: %s',
                pformat(message_dict),
                error
            )

    def format_timestamp_to_iso8601(self, timestamp):
        if self.config.timestamp_format:
            parsed_date = datetime.strptime(
                timestamp,
                self.config.timestamp_format
            )
        else:
            try:
                parsed_date = dateutil.parser.parse(timestamp, fuzzy=True)
            except Exception as error:
                logger.exception(
                    'There was an error while parsing timestamp %r: %s',
                    timestamp,
                    error
                )
                logger.error(
                    'Please try setting a timestamp format in logfile config'
                )
                parsed_date = datetime.utcnow()

        if parsed_date.tzinfo is not None:
            return parsed_date.astimezone(
                dateutil.tz.tzutc()
            ).strftime('%Y-%m-%dT%H:%M:%S.000Z')
        else:
            return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')

class Main(object):
    def __init__(self, logshipper_config, foreground=False,
                 pattern_config_class=PatternsConfigDir,
                 logfile_config_class=LogfileConfigFile):

        self.logshipper_config = logshipper_config
        self.pattern_config_class = pattern_config_class
        self.logfile_config_class = logfile_config_class
        self.logfile_configs = dict()
        self.patterns = dict()
        self.foreground = foreground
        self.stop_event = threading.Event()
        self.thread_list = list()
        self.sender_queue = Queue()
        self.persistence_queue = Queue()

        logger.debug(
            'Initialised with values:\n\n%s\n',
            pformat(self.__dict__)
        )

    def run(self):
        logger.info('Startup!')
        self.read_patterns()
        self.read_config_dir()

        pid_dirname = os.path.dirname(self.logshipper_config.pidfile)
        if not os.path.exists(pid_dirname):
            logger.debug(
                'Creating pid dir at %r', pid_dirname
            )
            os.mkdir(pid_dirname, 0750)

        pidfile = pidlockfile.PIDLockFile(self.logshipper_config.pidfile)
        if pidfile.is_locked():
            lockfile_pid = pidfile.read_pid()
            my_pid = os.getpid()
            if lockfile_pid != my_pid:
                raise RuntimeError(
                    'There is already a pid file at %s with pid %s.'
                    %(self.logshipper_config.pidfile, lockfile_pid)
                )

        context = daemon.DaemonContext(
            detach_process=self.foreground is False,
            pidfile=pidfile,
            uid=username2uid(self.logshipper_config.user),
            gid=groupname2gid(self.logshipper_config.group),
            stdout=sys.stdout if self.foreground is True else None,
            stderr=sys.stderr if self.foreground is True else None
        )
        files_preserve = list()
        for handler in logging.root.handlers:
            files_preserve.append(handler.stream.fileno())

        context.files_preserve = files_preserve
        context.signal_map = {
            signal.SIGTERM: self.shutdown,
            signal.SIGINT: self.shutdown,
            signal.SIGHUP: self.reload
        }

        with context:
            setproctitle.setproctitle(' '.join(sys.argv))
            #self.scheduler = LogshipperScheduler(
            #    threadpool=ThreadPool(
            #        max_threads=len(self.logfile_configs),
            #    )
            #)
            self.prepare_scheduler()
            self.prepare_fs_notifer()
            self.start_scheduler()
            self.start_notifier()

            for path, config in self.logfile_configs.iteritems():
                self.add_job_to_scheduler(path=path, config=config)

            while not self.stop_event.is_set():
                sleep(0.1)

    def prepare_scheduler(self):
        self.scheduler = LogshipperScheduler(
            threadpool=ThreadPool(
                max_threads=len(self.logfile_configs),
            )
        )


    def prepare_fs_notifer(self):
        callbacks = {
            'IN_MOVED_TO': self.add_job_to_scheduler,
            'IN_MOVED_FROM': self.remove_job_from_scheduler,
            'IN_CLOSE_WRITE': self.add_job_to_scheduler,
            'IN_DELETE': self.remove_job_from_scheduler,
            'IN_CREATE': self.add_job_to_scheduler,
        }
        self.watch_manager = WatchManager()
        self.notifier = ThreadedNotifier(
            self.watch_manager,
            default_proc_fun=ProcessConfigEvents(callbacks=callbacks)
        )
        self.watch_manager.add_watch(
            self.logshipper_config.configdir,
            pyinotify.IN_CLOSE_WRITE |\
            pyinotify.IN_DELETE |\
            pyinotify.IN_CREATE |\
            pyinotify.IN_MOVED_TO |\
            pyinotify.IN_MOVED_FROM,
            rec=False,
            auto_add=True
        )

    def reload(self, signum, sigframe):
        logger.info('Received signal %r', signum)
        logger.info('Reloading ...')
        old_logshipper_config = self.logshipper_config
        self.logshipper_config = LogshipperConfigFile(
            self.logshipper_config.path
        )

        if old_logshipper_config.loglevel != self.logshipper_config.loglevel:
            logger.info(
                'Setting loglevel from %s to %s',
                old_logshipper_config.loglevel,
                self.logshipper_config.loglevel
            )
            logging.getLogger('').setLevel(self.logshipper_config.loglevel)

        self.stop_notifier()
        self.stop_scheduler()
        self.read_patterns()
        self.read_config_dir()
        self.prepare_scheduler()
        self.start_scheduler()
        for path, config in self.logfile_configs.iteritems():
            self.add_job_to_scheduler(path=path, config=config)
        self.prepare_fs_notifer()
        self.start_notifier()
        logger.info('Reload is done ...')

    def start_scheduler(self):
        logger.debug('Starting scheduler ...')
        self.scheduler.start()

    def stop_scheduler(self):
        logger.debug('Shutting down scheduler ...')
        self.scheduler.shutdown()
        logger.debug('Scheduler stopped.')

    def start_notifier(self):
        logger.debug('Starting FS notifier ...')
        self.notifier.start()

    def stop_notifier(self):
        logger.debug('Shutting down FS notifier ...')
        self.notifier.stop()
        logger.debug('FS notifier stopped.')

    def shutdown(self, signum, sigframe):
        logger.info('Received signal %r', signum)
        logger.debug('Setting stop event.')
        self.stop_event.set()
        self.stop_notifier()
        self.stop_scheduler()
        logger.info('Stopping now!')
        sys.exit(0)

    def read_patterns(self):
        logger.debug('Re-reading patterns')
        self.patterns = dict(
            self.pattern_config_class(
                self.logshipper_config.patterns_dir
            ).items('patterns')
        )

    def read_config_dir(self):
        for path in iglob(
                os.path.join(self.logshipper_config.configdir, '*.cfg')
        ):
            path = os.path.abspath(path)
            try:
                config = self.logfile_config_class(
                    path,
                    statusdir=self.logshipper_config.statusdir,
                    patterns=self.patterns)
            except ConfigError as error:
                logger.exception(
                    'Error while loading logfile config from %r: %s',
                    path,
                    error
                )
            else:
                self.logfile_configs.update({path: config})
        logger.debug(
            'Collected logfile configs:\n\n%s\n',
            pformat(self.logfile_configs)
        )

    def add_job_to_scheduler(self, path=None, config=None):
        if (path or config) is None:
            raise ValueError(
                'Argument path or config must be set.'
            )

        if config is None:
            config = self.logfile_config_class(
                    path,
                    statusdir=self.logshipper_config.statusdir,
                    patterns=self.patterns)

        logger.info('Trying to delete job %r if it exists.', path)
        self.remove_job_from_scheduler(config.path)

        parser = FileParser(
            config,
            self.stop_event,
            supported_senders.get(self.logshipper_config.sender),
            dict(
                self.logshipper_config.items(self.logshipper_config.sender)
            ),
            self.logshipper_config.max_messages
        )
        self.scheduler.add_new_job(
            parser.run,
            config.path,
            self.logshipper_config.read_interval
        )

    def remove_job_from_scheduler(self, path):
        self.scheduler.remove_job(path)


def uid2username(uid):
    return pwd.getpwuid(uid).pw_name


def username2uid(username):
    return pwd.getpwnam(username).pw_uid


def groupname2gid(groupname):
    return grp.getgrnam(groupname).gr_gid


def gid2groupname(gid):
    return grp.getgrgid(gid).gr_name


def getopts():
    description = 'A Logfile Parser and data shipper for logstash.'
    parser = OptionParser(
        version='%prog ' + __version__,
        description=description
    )

    parser.add_option(
        '-f',
        '--foreground',
        default=False,
        action='store_true',
        help='Set this option to not Fork in Background. Default: %default'
    )

    parser.add_option(
        '-c',
        '--config',
        default='/etc/logshipper/logshipper.cfg',
        help='The path to the logshipper config itself. Default: %default'
    )

    options, args = parser.parse_args()
    options.config = os.path.abspath(options.config)

    return options, args


if __name__ == '__main__':
    options, args = getopts()
    logshipper_config = LogshipperConfigFile(options.config)

    logfile = os.path.join(
        logshipper_config.get('default', 'logdir'),
        'logshipper.log'
    )

    logging.basicConfig(
        format=BASE_FORMAT_STDOUT,
        level=logshipper_config.loglevel,
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout if options.foreground else open(os.devnull, 'w+', 1)
    )

    root_logger = logging.getLogger('')
    logger = logging.getLogger(os.path.basename(sys.argv[0]))

    logfile_handler = WatchedFileHandler(logshipper_config.logfile)
    formatter = logging.Formatter(
        BASE_FORMAT_STDOUT,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logfile_handler.setFormatter(formatter)
    root_logger.addHandler(logfile_handler)

    logger.debug(
        'Logshipper config is:\n\n%s\n',
        pformat(dict(logshipper_config.items('default')))
    )

    main = Main(logshipper_config, foreground=options.foreground)
    try:
        main.run()
    except Exception as error:
        logger.exception('Unhandled Exception: %s', error)
        os.kill(os.getpid(), signal.SIGTERM)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4