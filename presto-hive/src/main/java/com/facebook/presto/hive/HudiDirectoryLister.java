/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration.CachingJobConf;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.HiveFileIterator;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static com.facebook.presto.hive.HiveSessionProperties.isHudiMetadataEnabled;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;

public class HudiDirectoryLister
    implements DirectoryLister
{
    private static final Cache<Path, List<HiveFileInfo>> cache = CacheBuilder.newBuilder()
        .maximumWeight(100000000)
        .weigher((Weigher<Path, List<HiveFileInfo>>) (key, value) -> value.size())
        .expireAfterWrite(new Duration(15, TimeUnit.MINUTES).toMillis(), TimeUnit.MILLISECONDS)
        .recordStats()
        .build();

    private static final Cache<String, HoodieTableFileSystemView> fsvCache = CacheBuilder.newBuilder()
        .expireAfterWrite(new Duration(24, TimeUnit.HOURS).toMillis(), TimeUnit.MILLISECONDS)
        .recordStats()
        .build();

    private static final Logger log = Logger.get(HudiDirectoryLister.class);

    private final HoodieTableMetaClient metaClient;
    private final boolean metadataEnabled;
    private final Configuration conf;

    public HudiDirectoryLister(Configuration conf, ConnectorSession session, Table table)
    {
        this.conf = conf;
        this.metadataEnabled = isHudiMetadataEnabled(session);
        Configuration actualConfig = ((CachingJobConf) conf).getConfig();
        /*
        WrapperJobConf acts as a wrapper on top of the actual Configuration object. If `hive.copy-on-first-write-configuration-enabled`
        is set to true, the wrapped object is instance of CopyOnFirstWriteConfiguration.
         */
        if (actualConfig instanceof CopyOnFirstWriteConfiguration) {
            actualConfig = ((CopyOnFirstWriteConfiguration) actualConfig).getConfig();
        }
        this.metaClient = HoodieTableMetaClient.builder()
            .setConf(actualConfig)
            .setBasePath(table.getStorage().getLocation())
            .build();

        getHoodieFSV(table.getStorage().getLocation(), table);
    }

    public HoodieTableMetaClient getMetaClient()
    {
        return metaClient;
    }

    @Override
    public Iterator<HiveFileInfo> list(
        ExtendedFileSystem fileSystem,
        Table table,
        Path path,
        Optional<Partition> partition,
        NamenodeStats namenodeStats,
        HiveDirectoryContext hiveDirectoryContext)
    {
        List<HiveFileInfo> files = cache.getIfPresent(path);
        if (files != null) {
            return files.iterator();
        }

        HoodieTableFileSystemView fileSystemView = getHoodieFSV(table.getStorage().getLocation(), table);

        log.debug("Listing path using Hudi directory lister: %s", path.toString());
        return new HiveFileIterator(
            path,
            p -> new HudiFileInfoIterator(
                fileSystemView,
                metadataEnabled ? Optional.empty() : Optional.of(fileSystem.listStatus(p)),
                table.getStorage().getLocation(),
                path,
                p),
            namenodeStats,
            hiveDirectoryContext.getNestedDirectoryPolicy(),
            hiveDirectoryContext.isSkipEmptyFilesEnabled());
    }

    private HoodieTableFileSystemView getHoodieFSV(String s, Table table)
    {
        HoodieTableFileSystemView fsv = fsvCache.getIfPresent(s);
        if (fsv != null) {
            return fsv;
        }

        log.debug("Cache hit failed for fsv for table: " + table.getTableName());
        Configuration actualConfig = ((CachingJobConf) conf).getConfig();

        if (actualConfig instanceof CopyOnFirstWriteConfiguration) {
            actualConfig = ((CopyOnFirstWriteConfiguration) actualConfig).getConfig();
        }

        HoodieEngineContext engineContext = new HoodieLocalEngineContext(actualConfig);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
            .enable(metadataEnabled)
            .build();

        fsv = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
        fsvCache.put(table.getStorage().getLocation(), fsv);

        return fsv;
    }

    public static class HudiFileInfoIterator
        implements RemoteIterator<HiveFileInfo>
    {
        private final Iterator<HoodieBaseFile> hoodieBaseFileIterator;

        private Path path;

        private final List<HiveFileInfo> files = new ArrayList<>();

        public HudiFileInfoIterator(
            HoodieTableFileSystemView fileSystemView,
            Optional<FileStatus[]> fileStatuses,
            String tablePath,
            Path path,
            Path directory)
        {
            String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), directory);
            if (fileStatuses.isPresent()) {
                fileSystemView.addFilesToView(fileStatuses.get());
                this.hoodieBaseFileIterator = fileSystemView.fetchLatestBaseFiles(partition).iterator();
            }
            else {
                this.hoodieBaseFileIterator = fileSystemView.getLatestBaseFiles(partition).iterator();
            }
            this.path = path;
        }

        @Override
        public boolean hasNext()
        {
            boolean hasNext = hoodieBaseFileIterator.hasNext();
            if (!hasNext) {
                cache.put(path, ImmutableList.copyOf(files));
            }
            return hasNext;
        }

        @Override
        public HiveFileInfo next()
            throws IOException
        {
            FileStatus fileStatus = hoodieBaseFileIterator.next().getFileStatus();
            String[] name = {"localhost:" + DFS_DATANODE_DEFAULT_PORT};
            String[] host = {"localhost"};
            LocatedFileStatus hoodieFileStatus = new LocatedFileStatus(fileStatus,
                new BlockLocation[] {new BlockLocation(name, host, 0L, fileStatus.getLen())});

            HiveFileInfo next = createHiveFileInfo(hoodieFileStatus, Optional.empty());
            files.add(next);
            return next;
        }
    }
}
