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

    def update_file(self, file_path, *, local_change_time=None, remote_change_time=None):
        assert local_change_time or remote_change_time, 'local_change_time or remote_change_time should be provided'
        if file_path not in self.files_dict:
            self.files_dict[file_path] = {'local_change_time': None, 'remote_change_time': None}
        if local_change_time:
            self.files_dict[file_path]['local_change_time'] = local_change_time
        if remote_change_time:
            self.files_dict[file_path]['remote_change_time'] = remote_change_time

    def remove_file(self, file_path):
        assert file_path in self.files_dict, f'file: {file_path} does not exist in database'
        del self.files_dict[file_path]


class Changes:
    def __init__(self, local_path, remote_path, saved_files: Files, **remote_kwargs):
        self.local_path = local_path
        self.remote_path = remote_path
        self.saved_files = saved_files
        self.remote_kwargs = remote_kwargs
        self.changes = {}
        self.LOCAL = 'local'
        self.REMOTE = 'remote'
        self.NEW_FILE = 'new'
        self.UPDATE = 'update'
        self.DELETE = 'delete'

    def _new_change(self, file_path):
        self.changes[file_path] = {}

    def make_changes(self):
        for path, change in self.changes.items():
            if self.LOCAL in change and self.REMOTE in change:
                # both local and remote changed
                # cannot have both remote and local add and remove
                if change[self.LOCAL] == change[self.REMOTE]:
                    # change type is the same
                    if change[self.LOCAL] == self.UPDATE:
                        different, local_file_data, remote_file_data = self._file_different(path)
                        if different:
                            older = self._find_older_file(path)
                            if older == self.REMOTE:
                                self._update_remote_file(path, local_file_data)
                            else:
                                self._update_local_file(path, remote_file_data)
                        else:
                            main_logger.info(f'new save entry: no syncing needed for file {path}')
                            self.saved_files.update_file(path,
                                                         local_change_time=os.path.getmtime(self.local_path + path),
                                                         remote_change_time=smbclient.path.getmtime(
                                                             self.remote_path + '/' + path, **self.remote_kwargs))

    def _find_older_file(self, file_path):
        local_change_time = os.path.getmtime(self.local_path + '/' + file_path)
        remote_change_time = smbclient.path.getmtime(self.remote_path + '/' + file_path, **self.remote_kwargs)
        main_logger.info(f'new save entry: with sync'
                         f' local time: {local_change_time}, remote time: {remote_change_time}')
        if local_change_time > remote_change_time:
            # remote file is older
            return self.REMOTE
        else:
            # local file is older
            return self.LOCAL

    def _file_different(self, file_path):
        with smbclient.open_file(self.remote_path + '/' + file_path, 'r', **self.remote_kwargs) as remote_file:
            remote_file_data = remote_file.read()
        with open(self.local_path + file_path, 'r') as local_file:
            local_file_data = local_file.read()
        if remote_file_data == local_file_data:
            return True, None, None
        else:
            return False, local_file_data, remote_file_data

    def _remove_from_remote(self, file_path):
        print(f'_remove_from_remote: {file_path}')

    def _remove_from_local(self, file_path):
        print(f'_remove_from_local: {file_path}')

    def _update_local_file(self, file_path, data):
        print(f'_update_local_file: {file_path}')

    def _update_remote_file(self, file_path, data):
        print(f'_update_remote_file: {file_path}')

    def _add_local_file(self, file_path, data):
        print(f'_add_local_file: {file_path}')

    def _add_remote_file(self, file_path, data):
        print(f'_add_remote_file: {file_path}')

    def new_local(self, file):
        if file not in self.changes:
            self._new_change(file)
        elif self.changes[file].get(self.LOCAL):
            raise Exception('there cannot be more than one local change')
        self.changes[file][self.LOCAL] = self.NEW_FILE
        main_logger.info(f'new local file: {file}')

    def new_remote(self, file):
        if file not in self.changes:
            self._new_change(file)
        elif self.changes[file].get(self.REMOTE):
            raise Exception('there cannot be more than one remote change')
        self.changes[file][self.REMOTE] = self.NEW_FILE
        main_logger.info(f'new remote file: {file}')

    def updated_local(self, file):
        if file not in self.changes:
            self._new_change(file)
        elif self.changes[file].get(self.LOCAL):
            raise Exception('there cannot be more than one local change')
        self.changes[file][self.LOCAL] = self.UPDATE
        main_logger.info(f'updated local file: {file}')

    def updated_remote(self, file):
        if file not in self.changes:
            self._new_change(file)
        elif self.changes[file].get(self.REMOTE):
            raise Exception('there cannot be more than one remote change')
        self.changes[file][self.REMOTE] = self.UPDATE
        main_logger.info(f'updated remote file: {file}')

    def deleted_local(self, file):
        if file not in self.changes:
            self._new_change(file)
        elif self.changes[file].get(self.LOCAL):
            raise Exception('there cannot be more than one local change')
        self.changes[file][self.LOCAL] = self.DELETE
        main_logger.info(f'deleted local file: {file}')

    def deleted_remote(self, file):
        if file not in self.changes:
            self._new_change(file)
        elif self.changes[file].get(self.REMOTE):
            raise Exception('there cannot be more than one remote change')
        self.changes[file][self.REMOTE] = self.DELETE
        main_logger.info(f'deleted remote file: {file}')


class SmbSync:
    def __init__(self, server_name, username, password, server_port=445, save_file_path='./saves/main_save.csv'):
        self._changes = None
        self._connection = Connection(uuid.uuid4(), server_name, server_port)
        smbclient.ClientConfig(username=username, password=password)
        self._server_name = server_name
        self._port = server_port
        self._save_path = save_file_path
        self._current_files = Files()
        self._saved_files = Files()

    def sync_folder(self, share_name, remote_path, local_path):
        self._connection.connect(Dialects.SMB_3_0_2, timeout=10)
        full_remote_path = '//' + self._server_name + '/' + share_name + '/' + remote_path
        self._load_remote_files(full_remote_path)
        main_logger.debug('-' * 25 + 'finished remote' + '-' * 25)
        self._load_local_files(local_path)
        main_logger.debug('-' * 25 + 'finished local' + '-' * 25)
        self._load_saved_files()
        self._changes = Changes(local_path, full_remote_path, self._saved_files, port=self._port)
        self._compare(full_remote_path, local_path)
        self._changes.make_changes()
        self._connection.disconnect()

    def _load_saved_files(self):
        self._saved_files = Files()
        if os.path.exists(self._save_path):
            with open(self._save_path) as save_file:
                reader = csv.reader(save_file)
                for row in reader:
                    self._saved_files.update_file(row[0], local_change_time=row[1], remote_change_time=row[2])

    def _load_remote_files(self, remote_path, search_dir=''):
        for entry in smbclient.scandir(remote_path + '/' + search_dir, port=self._port):
            if entry.is_dir():
                main_logger.debug(f'searching remote: {search_dir}/{entry.name}')
                self._load_remote_files(remote_path, search_dir=search_dir + '/' + entry.name)
            else:
                main_logger.debug(f'found remote file: {search_dir}/{entry.name}')
                self._current_files.update_file(f'{search_dir}/{entry.name}',
                                                remote_change_time=entry.smb_info.change_time)

    def _load_local_files(self, local_path, search_dir=''):
        for entry in os.scandir(local_path + '/' + search_dir):
            if entry.is_dir():
                main_logger.debug(f'searching local: {search_dir}/{entry.name}')
                self._load_local_files(local_path, search_dir=search_dir + '/' + entry.name)
            else:
                main_logger.debug(f'found local file: {search_dir}/{entry.name}')
                self._current_files.update_file(f'{search_dir}/{entry.name}',
                                                local_change_time=entry)

    def _compare(self, remote_path, local_path):
        for file_path, file_attrs in self._current_files.files_dict.items():
            if file_path in self._saved_files.files_dict:
                # file has been synced before
                if file_attrs['local_change_time'] != self._saved_files.files_dict[file_path]['local_change_time']:
                    # local file changed
                    if file_attrs['local_change_time'] is None:
                        self._changes.deleted_local(file_path)
                    else:
                        self._changes.updated_local(file_path)
                if file_attrs['remote_change_time'] != self._saved_files.files_dict[file_path]['remote_change_time']:
                    # remote file changed
                    if file_attrs['remote_change_time'] is None:
                        self._changes.deleted_remote(file_path)
                    else:
                        self._changes.updated_remote(file_path)
            else:
                # file has not been synced before
                if file_attrs['remote_change_time'] is None:
                    # file exists only in local
                    self._changes.new_local(file_path)
                elif file_attrs['local_change_time'] is None:
                    # file exists only in remote
                    self._changes.new_remote(file_path)
                else:
                    # file exists in both remote and local
                    self._changes.updated_local(file_path)
                    self._changes.updated_remote(file_path)

        for file_path in self._saved_files.files_dict:
            if file_path not in self._current_files.files_dict:
                main_logger.info(f'deleted file from both local and server: {file_path}')
                self._saved_files.remove_file(file_path)
