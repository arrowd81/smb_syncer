from config import remote_dir, local_dir, password, username, share_name, server_name, server_port, save_file_path

from syncer import SmbSync

smb_sync = SmbSync(server_name, username, password, server_port, save_file_path)
smb_sync.sync_folder(share_name, remote_dir, local_dir)
