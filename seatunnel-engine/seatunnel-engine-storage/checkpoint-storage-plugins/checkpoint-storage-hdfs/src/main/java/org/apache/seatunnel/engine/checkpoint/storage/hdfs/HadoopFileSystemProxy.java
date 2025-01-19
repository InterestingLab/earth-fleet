/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HadoopFileSystemProxy implements Serializable, Closeable {

    /** hdfs kerberos principal( is optional) */
    private static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

    private static final String KEB5_PATH = "krb5Path";
    private static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    private static final String REMOTE_USER = "remoteUser";

    private transient UserGroupInformation userGroupInformation;
    private transient FileSystem fileSystem;

    private transient Configuration configuration;
    private boolean isAuthTypeKerberos;

    public HadoopFileSystemProxy(@NonNull Configuration configuration) {
        this.configuration = configuration;
        // eager initialization
        initialize();
    }

    public FileSystem getFileSystem() {
        if (fileSystem == null) {
            initialize();
        }
        return fileSystem;
    }

    public boolean fileExist(@NonNull Path filePath) throws IOException {
        return execute(() -> getFileSystem().exists(filePath));
    }

    public boolean isFile(@NonNull String filePath) throws IOException {
        return execute(() -> getFileSystem().getFileStatus(new Path(filePath)).isFile());
    }

    public void deleteFile(@NonNull Path filePath) throws IOException {
        execute(
                () -> {
                    if (getFileSystem().exists(filePath)) {
                        if (!getFileSystem().delete(filePath, true)) {
                            throw new CheckpointStorageException(
                                    String.format(
                                            "Failed to delete checkpoint data, file: %s",
                                            filePath));
                        }
                    }
                    return Void.class;
                });
    }

    public void renameFile(
            @NonNull Path oldFilePath,
            @NonNull Path newFilePath,
            boolean removeWhenNewFilePathExist)
            throws IOException {
        execute(
                () -> {
                    if (!fileExist(oldFilePath)) {
                        log.warn(
                                "rename file :["
                                        + oldFilePath
                                        + "] to ["
                                        + newFilePath
                                        + "] already finished in the last commit, skip");
                        return Void.class;
                    }

                    if (removeWhenNewFilePathExist) {
                        if (fileExist(newFilePath)) {
                            getFileSystem().delete(oldFilePath, true);
                            log.info("Delete already file: {}", newFilePath);
                        }
                    }
                    if (!fileExist(newFilePath.getParent())) {
                        createDir(newFilePath.getParent().toString());
                    }

                    if (getFileSystem().rename(oldFilePath, newFilePath)) {
                        log.info(
                                "rename file :["
                                        + oldFilePath
                                        + "] to ["
                                        + newFilePath
                                        + "] finish");
                    } else {
                        throw new CheckpointStorageException(
                                String.format(
                                        "Failed to rename checkpoint data, file: %s -> %s",
                                        oldFilePath, newFilePath));
                    }
                    return Void.class;
                });
    }

    public void createDir(@NonNull String filePath) throws IOException {
        execute(
                () -> {
                    Path dfs = new Path(filePath);
                    if (!getFileSystem().mkdirs(dfs)) {
                        throw new CheckpointStorageException(
                                String.format(
                                        "Failed to write checkpoint data, file: %s", filePath));
                    }
                    return Void.class;
                });
    }

    public List<LocatedFileStatus> listFile(Path path) throws IOException {
        return execute(
                () -> {
                    List<LocatedFileStatus> fileList = new ArrayList<>();
                    if (!fileExist(path)) {
                        return fileList;
                    }
                    RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                            getFileSystem().listFiles(path, false);
                    while (locatedFileStatusRemoteIterator.hasNext()) {
                        fileList.add(locatedFileStatusRemoteIterator.next());
                    }
                    return fileList;
                });
    }

    public List<Path> getAllSubFiles(@NonNull Path filePath) throws IOException {
        return execute(
                () -> {
                    List<Path> pathList = new ArrayList<>();
                    if (!fileExist(filePath)) {
                        return pathList;
                    }
                    FileStatus[] status = getFileSystem().listStatus(filePath);
                    if (status != null) {
                        for (FileStatus fileStatus : status) {
                            if (fileStatus.isDirectory()) {
                                pathList.add(fileStatus.getPath());
                            }
                        }
                    }
                    return pathList;
                });
    }

    public FileStatus[] listStatus(String filePath) throws IOException {
        return execute(() -> getFileSystem().listStatus(new Path(filePath)));
    }

    public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
        return execute(() -> getFileSystem().listStatus(f, filter));
    }

    public FileStatus getFileStatus(String filePath) throws IOException {
        return execute(() -> getFileSystem().getFileStatus(new Path(filePath)));
    }

    public FSDataOutputStream getOutputStream(Path filePath) throws IOException {
        return execute(() -> getFileSystem().create(filePath, false));
    }

    public FSDataOutputStream getOutputStream(Path filePath, boolean overwrite) throws IOException {
        return execute(() -> getFileSystem().create(filePath, overwrite));
    }

    public FSDataInputStream getInputStream(String filePath) throws IOException {
        return execute(() -> getFileSystem().open(new Path(filePath)));
    }

    @SneakyThrows
    public <T> T doWithHadoopAuth(HadoopLoginFactory.LoginFunction<T> loginFunction) {
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            return HadoopLoginFactory.loginWithKerberos(
                    configuration,
                    configuration.get(KEB5_PATH),
                    configuration.get(KERBEROS_PRINCIPAL),
                    configuration.get(KERBEROS_KEYTAB_FILE_PATH),
                    loginFunction);
        }
        if (enableRemoteUser()) {
            return HadoopLoginFactory.loginWithRemoteUser(
                    configuration, configuration.get(REMOTE_USER), loginFunction);
        }
        return loginFunction.run(configuration, UserGroupInformation.getCurrentUser());
    }

    @Override
    public void close() throws IOException {
        try {
            if (userGroupInformation != null && isAuthTypeKerberos) {
                userGroupInformation.logoutUserFromKeytab();
            }
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    @SneakyThrows
    private void initialize() {
        if (enableKerberos()) {
            configuration.set("hadoop.security.authentication", "kerberos");
            initializeWithKerberosLogin();
            isAuthTypeKerberos = true;
            return;
        }
        if (enableRemoteUser()) {
            initializeWithRemoteUserLogin();
            isAuthTypeKerberos = true;
            return;
        }
        fileSystem = FileSystem.get(configuration);
        fileSystem.setWriteChecksum(false);
        isAuthTypeKerberos = false;
    }

    private boolean enableKerberos() {
        boolean kerberosPrincipalEmpty = StringUtils.isBlank(configuration.get(KERBEROS_PRINCIPAL));
        boolean kerberosKeytabPathEmpty =
                StringUtils.isBlank(configuration.get(KERBEROS_KEYTAB_FILE_PATH));
        if (kerberosKeytabPathEmpty && kerberosPrincipalEmpty) {
            return false;
        }
        if (!kerberosPrincipalEmpty && !kerberosKeytabPathEmpty) {
            return true;
        }
        if (kerberosPrincipalEmpty) {
            throw new IllegalArgumentException("Please set kerberosPrincipal");
        }
        throw new IllegalArgumentException("Please set kerberosKeytabPath");
    }

    private void initializeWithKerberosLogin() throws IOException, InterruptedException {
        Pair<UserGroupInformation, FileSystem> pair =
                HadoopLoginFactory.loginWithKerberos(
                        configuration,
                        configuration.get(KEB5_PATH),
                        configuration.get(KERBEROS_PRINCIPAL),
                        configuration.get(KERBEROS_KEYTAB_FILE_PATH),
                        (configuration, userGroupInformation) -> {
                            this.userGroupInformation = userGroupInformation;
                            this.fileSystem = FileSystem.get(configuration);
                            return Pair.of(userGroupInformation, fileSystem);
                        });
        userGroupInformation = pair.getKey();
        fileSystem = pair.getValue();
        fileSystem.setWriteChecksum(false);
        log.info(
                "Create FileSystem success with Kerberos: {}.",
                configuration.get(KERBEROS_PRINCIPAL));
    }

    private boolean enableRemoteUser() {
        return StringUtils.isNotBlank(configuration.get(REMOTE_USER));
    }

    private void initializeWithRemoteUserLogin() throws Exception {
        final Pair<UserGroupInformation, FileSystem> pair =
                HadoopLoginFactory.loginWithRemoteUser(
                        configuration,
                        configuration.get(REMOTE_USER),
                        (configuration, userGroupInformation) -> {
                            this.userGroupInformation = userGroupInformation;
                            this.fileSystem = FileSystem.get(configuration);
                            return Pair.of(userGroupInformation, fileSystem);
                        });
        log.info("Create FileSystem success with RemoteUser: {}.", configuration.get(REMOTE_USER));
        userGroupInformation = pair.getKey();
        fileSystem = pair.getValue();
        fileSystem.setWriteChecksum(false);
    }

    private <T> T execute(PrivilegedExceptionAction<T> action) throws IOException {
        // The execute method is used to handle privileged actions, ensuring that the correct
        // user context (Kerberos or otherwise) is applied when performing file system operations.
        // This is necessary to maintain security and proper access control in a Hadoop environment.
        // If kerberos is disabled, the action is run directly. If kerberos is enabled, the action
        // is run as a privileged action using the doAsPrivileged method.
        if (isAuthTypeKerberos) {
            return doAsPrivileged(action);
        } else {
            try {
                return action.run();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private <T> T doAsPrivileged(PrivilegedExceptionAction<T> action) throws IOException {
        if (fileSystem == null || userGroupInformation == null) {
            initialize();
        }

        try {
            return userGroupInformation.doAs(action);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
