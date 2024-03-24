import csv
import logging
import os.path
import uuid

import smbclient
from smbprotocol import Dialects
from smbprotocol.connection import Connection

# logger
main_logger = logging.getLogger(__name__)

main_logger.setLevel(logging.DEBUG)

handler = logging.FileHandler('syncer.log', "w")

formatter = logging.Formatter("%(asctime)-20s : %(levelname)s : %(message)s")

handler.setFormatter(formatter)

main_logger.addHandler(handler)


class Files:
    def __init__(self):
        self.files_dict = {}

    def add_file(self, file_path, *, local_change_time=None, remote_change_time=None):
        assert local_change_time or remote_change_time, 'local_change_time or remote_change_time should be provided'
        if file_path not in self.files_dict:
            self.files_dict[file_path] = {'local_change_time': None, 'remote_change_time': None}
        if local_change_time:
            self.files_dict[file_path]['local_change_time'] = local_change_time
        if remote_change_time:
            self.files_dict[file_path]['remote_change_time'] = remote_change_time


class Changes:
    def __init__(self):
        self.new_remote_files = []
        self.new_local_files = []
        self.updated_remote_files = []
        self.updated_local_files = []
        self.deleted_remote_files = []
        self.deleted_local_files = []

    def new_local(self, file):
        self.new_local_files.append(file)
        main_logger.info(f'new local file: {file}')

    def new_remote(self, file):
        self.new_remote_files.append(file)
        main_logger.info(f'new remote file: {file}')

    def updated_local(self, file):
        self.updated_local_files.append(file)
        main_logger.info(f'updated local file: {file}')

    def updated_remote(self, file):
        self.updated_remote_files.append(file)
        main_logger.info(f'updated remote file: {file}')

    def deleted_local(self, file):
        self.deleted_local_files.append(file)
        main_logger.info(f'deleted local file: {file}')

    def deleted_remote(self, file):
        self.deleted_remote_files.append(file)
        main_logger.info(f'deleted remote file: {file}')


class SmbSync:
    def __init__(self, server_name, username, password, server_port=445, save_file_path='./saves/main_save.csv'):
        self._connection = Connection(uuid.uuid4(), server_name, server_port)
        smbclient.ClientConfig(username=username, password=password)
        self._server_name = server_name
        self._port = server_port
        self._save_path = save_file_path
        self._current_files = Files()
        self._saved_files = Files()
        self._changes = Changes()

    def sync_folder(self, share_name, remote_path, local_path):
        self._connection.connect(Dialects.SMB_3_0_2, timeout=10)
        full_remote_path = '//' + self._server_name + '/' + share_name + '/' + remote_path
        self._load_remote_files(full_remote_path)
        main_logger.debug('-' * 25 + 'finished remote' + '-' * 25)
        self._load_local_files(local_path)
        main_logger.debug('-' * 25 + 'finished local' + '-' * 25)
        self._load_saved_files()
        self._compare(full_remote_path, local_path)
        self._connection.disconnect()

    def _load_saved_files(self):
        self._saved_files = Files()
        if os.path.exists(self._save_path):
            with open(self._save_path) as save_file:
                reader = csv.reader(save_file)
                for row in reader:
                    self._saved_files.add_file(row[0], local_change_time=row[1], remote_change_time=row[2])

    def _load_remote_files(self, remote_path, search_dir=''):
        for entry in smbclient.scandir(remote_path + '/' + search_dir, port=self._port):
            if entry.is_dir():
                main_logger.debug(f'searching remote: {search_dir}/{entry.name}')
                self._load_remote_files(remote_path, search_dir=search_dir + '/' + entry.name)
            else:
                main_logger.debug(f'found remote file: {search_dir}/{entry.name}')
                self._current_files.add_file(f'{search_dir}/{entry.name}',
                                             remote_change_time=entry.smb_info.change_time)

    def _load_local_files(self, local_path, search_dir=''):
        for entry in os.scandir(local_path + '/' + search_dir):
            if entry.is_dir():
                main_logger.debug(f'searching local: {search_dir}/{entry.name}')
                self._load_local_files(local_path, search_dir=search_dir + '/' + entry.name)
            else:
                main_logger.debug(f'found local file: {search_dir}/{entry.name}')
                self._current_files.add_file(f'{search_dir}/{entry.name}',
                                             local_change_time=entry)

    def _compare(self, remote_path, local_path):
        for file_path, file_attrs in self._current_files.files_dict.items():
            if file_path in self._saved_files.files_dict:
                if file_attrs['local_change_time'] != self._saved_files.files_dict[file_path]['local_change_time']:
                    if file_attrs['local_change_time'] is None:
                        self._changes.deleted_local(file_path)
                    else:
                        self._changes.updated_local(file_path)
                if file_attrs['remote_change_time'] != self._saved_files.files_dict[file_path]['remote_change_time']:
                    if file_attrs['remote_change_time'] is None:
                        self._changes.deleted_remote(file_path)
                    else:
                        self._changes.updated_remote(file_path)
            else:
                if file_attrs['remote_change_time'] is None:
                    self._saved_files.add_file(file_path, local_change_time=file_attrs['local_change_time'])
                    self._changes.new_local(file_path)
                elif file_attrs['local_change_time'] is None:
                    self._saved_files.add_file(file_path, remote_change_time=file_attrs['remote_change_time'])
                    self._changes.new_remote(file_path)
                else:
                    with smbclient.open_file(remote_path + '/' + file_path, 'r', port=self._port) as remote_file:
                        remote_file_data = remote_file.read()
                    with open(local_path + file_path, 'r') as local_file:
                        local_file_data = local_file.read()
                    if remote_file_data == local_file_data:
                        main_logger.debug('new save entry: no syncing needed')
                        self._saved_files.add_file(file_path,
                                                   local_change_time=os.path.getmtime(local_path + file_path),
                                                   remote_change_time=smbclient.path.getmtime(
                                                       remote_path + '/' + file_path,
                                                       port=self._port))
                    else:
                        local_change_time = os.path.getmtime(local_path + '/' + file_path)
                        remote_change_time = smbclient.path.getmtime(remote_path + '/' + file_path, port=self._port)
                        main_logger.debug(
                            f'new save entry: with sync local time: {local_change_time}, remote time: {remote_change_time}')
                        if local_change_time > remote_change_time:
                            self._saved_files.add_file(file_path, local_change_time=local_change_time)
                            self._changes.updated_local(file_path)
                        else:
                            self._saved_files.add_file(file_path, remote_change_time=remote_change_time)
                            self._changes.updated_remote(file_path)

        for saved_file in self._saved_files.files_dict:
            if saved_file not in self._current_files.files_dict:
                # todo: deleted files from remote and local
                pass
