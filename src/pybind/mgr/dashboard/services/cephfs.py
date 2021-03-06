# -*- coding: utf-8 -*-
from __future__ import absolute_import

from contextlib import contextmanager

import datetime
import os
import six
import cephfs

from .. import mgr, logger


class CephFS(object):
    @classmethod
    def list_filesystems(cls):
        fsmap = mgr.get("fs_map")
        return [{'id': fs['id'], 'name': fs['mdsmap']['fs_name']}
                for fs in fsmap['filesystems']]

    @classmethod
    def fs_name_from_id(cls, fs_id):
        """
        Get the filesystem name from ID.
        :param fs_id: The filesystem ID.
        :type fs_id: int | str
        :return: The filesystem name or None.
        :rtype: str | None
        """
        fs_map = mgr.get("fs_map")
        fs_info = list(filter(lambda x: str(x['id']) == str(fs_id),
                              fs_map['filesystems']))
        if not fs_info:
            return None
        return fs_info[0]['mdsmap']['fs_name']

    def __init__(self, fs_name=None):
        logger.debug("[CephFS] initializing cephfs connection")
        self.cfs = cephfs.LibCephFS(rados_inst=mgr.rados)
        logger.debug("[CephFS] mounting cephfs filesystem: %s", fs_name)
        if fs_name:
            self.cfs.mount(filesystem_name=fs_name)
        else:
            self.cfs.mount()
        logger.debug("[CephFS] mounted cephfs filesystem")

    def __del__(self):
        logger.debug("[CephFS] shutting down cephfs filesystem")
        self.cfs.shutdown()

    @contextmanager
    def opendir(self, dirpath):
        d = None
        try:
            d = self.cfs.opendir(dirpath)
            yield d
        finally:
            if d:
                self.cfs.closedir(d)

    def ls_dir(self, path, level):
        """
        List directories of specified path.
        :param path: The root directory path.
        :type path: str | bytes
        :param level: The number of steps to go down the directory tree.
        :type level: int
        :return: A list of directory paths (bytes encoded). The specified
            root directory is also included.
            Example:
            ls_dir('/photos', 1) => [
                b'/photos', b'/photos/flowers', b'/photos/cars'
            ]
        :rtype: list
        """
        if isinstance(path, six.string_types):
            path = path.encode()
        logger.debug("[CephFS] get_dir_list dir_path=%s level=%s",
                     path, level)
        if level == 0:
            return [path]
        logger.debug("[CephFS] opening dir_path=%s", path)
        with self.opendir(path) as d:
            dent = self.cfs.readdir(d)
            paths = [path]
            while dent:
                logger.debug("[CephFS] found entry=%s", dent.d_name)
                if dent.d_name in [b'.', b'..']:
                    dent = self.cfs.readdir(d)
                    continue
                if dent.is_dir():
                    logger.debug("[CephFS] found dir=%s", dent.d_name)
                    subdir_path = os.path.join(path, dent.d_name)
                    paths.extend(self.ls_dir(subdir_path, level - 1))
                dent = self.cfs.readdir(d)
        return paths

    def dir_exists(self, path):
        try:
            with self.opendir(path):
                return True
        except cephfs.ObjectNotFound:
            return False

    def mk_dirs(self, path):
        """
        Create a directory.
        :param path: The path of the directory.
        """
        if path == os.sep:
            raise Exception('Cannot create root directory "/"')
        if self.dir_exists(path):
            return
        logger.info("[CephFS] Creating directory: %s", path)
        self.cfs.mkdirs(path, 0o755)

    def rm_dir(self, path):
        """
        Remove a directory.
        :param path: The path of the directory.
        """
        if path == os.sep:
            raise Exception('Cannot remove root directory "/"')
        if not self.dir_exists(path):
            return
        logger.info("[CephFS] Removing directory: %s", path)
        self.cfs.rmdir(path)

    def mk_snapshot(self, path, name=None, mode=0o755):
        """
        Create a snapshot.
        :param path: The path of the directory.
        :type path: str
        :param name: The name of the snapshot. If not specified,
            a name using the current time in RFC3339 UTC format
            will be generated.
        :type name: str | None
        :param mode: The permissions the directory should have
            once created.
        :type mode: int
        :return: Returns the name of the snapshot.
        :rtype: str
        """
        if name is None:
            now = datetime.datetime.now()
            tz = now.astimezone().tzinfo
            name = now.replace(tzinfo=tz).isoformat('T')
        client_snapdir = self.cfs.conf_get('client_snapdir')
        snapshot_path = os.path.join(path, client_snapdir, name)
        logger.info("[CephFS] Creating snapshot: %s", snapshot_path)
        self.cfs.mkdir(snapshot_path, mode)
        return name

    def ls_snapshots(self, path):
        """
        List snapshots for the specified path.
        :param path: The path of the directory.
        :type path: str
        :return: A list of dictionaries containing the name and the
          creation time of the snapshot.
        :rtype: list
        """
        result = []
        client_snapdir = self.cfs.conf_get('client_snapdir')
        path = os.path.join(path, client_snapdir).encode()
        with self.opendir(path) as d:
            dent = self.cfs.readdir(d)
            while dent:
                if dent.is_dir():
                    if dent.d_name not in [b'.', b'..']:
                        snapshot_path = os.path.join(path, dent.d_name)
                        stat = self.cfs.stat(snapshot_path)
                        result.append({
                            'name': dent.d_name.decode(),
                            'path': snapshot_path.decode(),
                            'created': '{}Z'.format(stat.st_ctime.isoformat('T'))
                        })
                dent = self.cfs.readdir(d)
        return result

    def rm_snapshot(self, path, name):
        """
        Remove a snapshot.
        :param path: The path of the directory.
        :type path: str
        :param name: The name of the snapshot.
        :type name: str
        """
        client_snapdir = self.cfs.conf_get('client_snapdir')
        snapshot_path = os.path.join(path, client_snapdir, name)
        logger.info("[CephFS] Removing snapshot: %s", snapshot_path)
        self.cfs.rmdir(snapshot_path)

    def get_quotas(self, path):
        """
        Get the quotas of the specified path.
        :param path: The path of the directory/file.
        :type path: str
        :return: Returns a dictionary containing 'max_bytes'
            and 'max_files'.
        :rtype: dict
        """
        try:
            max_bytes = int(self.cfs.getxattr(path, 'ceph.quota.max_bytes'))
        except cephfs.NoData:
            max_bytes = 0
        try:
            max_files = int(self.cfs.getxattr(path, 'ceph.quota.max_files'))
        except cephfs.NoData:
            max_files = 0
        return {'max_bytes': max_bytes, 'max_files': max_files}

    def set_quotas(self, path, max_bytes=None, max_files=None):
        """
        Set the quotas of the specified path.
        :param path: The path of the directory/file.
        :type path: str
        :param max_bytes: The byte limit.
        :type max_bytes: int | None
        :param max_files: The file limit.
        :type max_files: int | None
        """
        self.cfs.setxattr(path, 'ceph.quota.max_bytes',
                          str(max_bytes if max_bytes else 0).encode(), 0)
        self.cfs.setxattr(path, 'ceph.quota.max_files',
                          str(max_files if max_files else 0).encode(), 0)
