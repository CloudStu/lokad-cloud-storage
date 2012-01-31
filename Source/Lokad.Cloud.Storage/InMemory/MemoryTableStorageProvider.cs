#region Copyright (c) Lokad 2009-2011
// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Linq;
using Lokad.Cloud.Storage.Azure;
using System.IO;

namespace Lokad.Cloud.Storage.InMemory
{
    /// <summary>Mock in-memory TableStorage Provider.</summary>
    /// <remarks>
    /// All the methods of <see cref="MemoryTableStorageProvider"/> are thread-safe.
    /// </remarks>
    public class MemoryTableStorageProvider : ITableStorageProvider
    {
        /// <summary>In memory table storage : entries per table (designed for simplicity instead of performance)</summary>
        readonly Dictionary<string, List<MockTableEntry>> _tables;

        /// <summary>Formatter as requiered to handle FatEntities.</summary>
        internal IDataSerializer DataSerializer { get; set; }

        /// <summary>Use the in-memory storage provider to store overflowing entities</summary>
        internal IBlobStorageProvider _blobStorage;

        /// <summary>naive global lock to make methods thread-safe.</summary>
        readonly object _syncRoot;

        int _nextETag;

        /// <summary>
        /// Constructor for <see cref="MemoryTableStorageProvider"/>.
        /// </summary>
        public MemoryTableStorageProvider()
        {
            _tables = new Dictionary<string, List<MockTableEntry>>();
            _syncRoot = new object();
            DataSerializer = new CloudFormatter();
            _blobStorage = CloudStorage.ForInMemoryStorage().BuildBlobStorage();
        }

        /// <see cref="ITableStorageProvider.CreateTable"/>
        public bool CreateTable(string tableName)
        {
            lock (_syncRoot)
            {
                if (_tables.ContainsKey(tableName))
                {
                    //If the table already exists: return false.
                    return false;
                }

                //create table return true.
                _tables.Add(tableName, new List<MockTableEntry>());
                return true;
            }
        }

        /// <see cref="ITableStorageProvider.DeleteTable"/>
        public bool DeleteTable(string tableName)
        {
            lock (_syncRoot)
            {
                if (_tables.ContainsKey(tableName))
                {
                    //If the table exists remove it.
                    _tables.Remove(tableName);
                    return true;
                }
                
                //Can not remove an unexisting table.
                return false;
            }
        }

        /// <see cref="ITableStorageProvider.GetTables"/>
        public IEnumerable<string> GetTables()
        {
            lock (_syncRoot)
            {
                return _tables.Keys;
            }
        }

        /// <see cref="ITableStorageProvider.Get{T}(string)"/>
        IEnumerable<CloudEntity<T>> GetInternal<T>(string tableName, Func<MockTableEntry,bool> predicate)
        {
            lock (_syncRoot)
            {
                if (!_tables.ContainsKey(tableName))
                {
                    return new List<CloudEntity<T>>();
                }

                return from entry in _tables[tableName]
                       where predicate(entry)
                       select DeserializeFatEntity<T>(entry.Value, DataSerializer, entry.ETag);
            }
        }

        /// <see cref="ITableStorageProvider.Get{T}(string)"/>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName)
        {
            return GetInternal<T>(tableName, entry => true);
        }

        /// <see cref="ITableStorageProvider.Get{T}(string,string)"/>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey)
        {
            return GetInternal<T>(tableName, entry => entry.PartitionKey == partitionKey);
        }

        /// <see cref="ITableStorageProvider.Get{T}(string,string,string,string)"/>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey, string startRowKey, string endRowKey)
        {
            var isInRange = string.IsNullOrEmpty(endRowKey)
                ? (Func<string, bool>)(rowKey => string.Compare(startRowKey, rowKey) <= 0)
                : (rowKey => string.Compare(startRowKey, rowKey) <= 0 && string.Compare(rowKey, endRowKey) < 0);

            return GetInternal<T>(tableName, entry => entry.PartitionKey == partitionKey && isInRange(entry.RowKey))
                .OrderBy(entity => entity.RowKey);
        }

        /// <see cref="ITableStorageProvider.Get{T}(string,string,System.Collections.Generic.IEnumerable{string})"/>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            var keys = new HashSet<string>(rowKeys);
            return GetInternal<T>(tableName, entry => entry.PartitionKey == partitionKey && keys.Contains(entry.RowKey));
        }

        /// <see cref="ITableStorageProvider.Insert{T}"/>
        public void Insert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            lock (_syncRoot)
            {
                List<MockTableEntry> entries;
                if (!_tables.TryGetValue(tableName, out entries))
                {
                    _tables.Add(tableName, entries = new List<MockTableEntry>());
                }

                // verify valid data BEFORE inserting them
                if (entities.Join(entries, u => ToId(u), ToId, (u, v) => true).Any())
                {
                    throw new DataServiceRequestException("INSERT: key conflict.");
                }
                if (entities.GroupBy(e => ToId(e)).Any(id => id.Count() != 1))
                {
                    throw new DataServiceRequestException("INSERT: duplicate keys.");
                }

                // ok, we can insert safely now
                foreach (var entity in entities)
                {
                    var etag = (_nextETag++).ToString();
                    entity.ETag = etag;
                    entries.Add(new MockTableEntry
                        {
                            PartitionKey = entity.PartitionKey,
                            RowKey = entity.RowKey,
                            ETag = etag,
                            Value = SerializeFatEntity(tableName, entity, DataSerializer)
                        });
                }
            }
        }

        /// <see cref="ITableStorageProvider.Update{T}"/>
        public void Update<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            lock (_syncRoot)
            {
                List<MockTableEntry> entries;
                if (!_tables.TryGetValue(tableName, out entries))
                {
                    throw new DataServiceRequestException("UPDATE: table not found.");
                }

                // verify valid data BEFORE updating them
                if (entities.GroupJoin(entries, u => ToId(u), ToId, (u, vs) => vs.Count(entry => force || u.ETag == null || entry.ETag == u.ETag)).Any(c => c != 1))
                {
                    throw new DataServiceRequestException("UPDATE: key not found or etag conflict.");
                }
                if (entities.GroupBy(e => ToId(e)).Any(id => id.Count() != 1))
                {
                    throw new DataServiceRequestException("UPDATE: duplicate keys.");
                }

                // ok, we can update safely now
                foreach (var entity in entities)
                {
                    var etag = (_nextETag++).ToString();
                    entity.ETag = etag;
                    var index = entries.FindIndex(entry => entry.PartitionKey == entity.PartitionKey && entry.RowKey == entity.RowKey);
                    entries[index] = new MockTableEntry
                        {
                            PartitionKey = entity.PartitionKey,
                            RowKey = entity.RowKey,
                            ETag = etag,
                            Value = SerializeFatEntity(tableName, entity, DataSerializer)
                        };
                }
            }
        }
        /// <see cref="ITableStorageProvider.Update{T}"/>
        public void Upsert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            lock (_syncRoot)
            {
                // deleting all existing entities
                foreach (var g in entities.GroupBy(e => e.PartitionKey))
                {
                    Delete<T>(tableName, g.Key, g.Select(e => e.RowKey));
                }

                // inserting all entities
                Insert(tableName, entities);
            }
        }

        /// <see cref="ITableStorageProvider.Delete{T}(string,string,IEnumerable{string})"/>
        public void Delete<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            lock (_syncRoot)
            {
                List<MockTableEntry> entries;
                if (!_tables.TryGetValue(tableName, out entries))
                {
                    return;
                }

                var keys = new HashSet<string>(rowKeys);
                entries.RemoveAll(entry => entry.PartitionKey == partitionKey && keys.Contains(entry.RowKey));

                foreach (var blobRef in keys.Select(k => new OverflowingFatEntityBlobName<T>(tableName, partitionKey, k)))
                    _blobStorage.DeleteBlobIfExist(blobRef);
            }
        }

        /// <remarks></remarks>
        public void Delete<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            lock (_syncRoot)
            {
                List<MockTableEntry> entries;
                if (!_tables.TryGetValue(tableName, out entries))
                {
                    return;
                }

                var entityList = entities.ToList();

                // verify valid data BEFORE deleting them
                if (entityList.Join(entries, u => ToId(u), ToId, (u, v) => force || u.ETag == null || u.ETag == v.ETag).Any(c => !c))
                {
                    throw new DataServiceRequestException("DELETE: etag conflict.");
                }

                // ok, we can delete safely now
                entries.RemoveAll(entry => entityList.Any(entity => entity.PartitionKey == entry.PartitionKey && entity.RowKey == entry.RowKey));

                foreach (var blobRef in entities.Select(e => new OverflowingFatEntityBlobName<T>(tableName, e.PartitionKey, e.RowKey)))
                    _blobStorage.DeleteBlobIfExist(blobRef);
            }
        }

        private FatEntity SerializeFatEntity<T>(string tableName, CloudEntity<T> cloudEntity, IDataSerializer serializer)
        {
            FatEntity fatEntity = new FatEntity
            {
                PartitionKey = cloudEntity.PartitionKey,
                RowKey = cloudEntity.RowKey,
                Timestamp = cloudEntity.Timestamp
            };

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(cloudEntity.Value, stream, typeof(T));

                // Caution: MaxMessageSize is not related to the number of bytes
                // but the number of characters when Base64-encoded:

                if (stream.Length >= FatEntity.MaxByteCapacity)
                {
                    fatEntity.SetData(PutOverflowingFatEntityAndWrap(tableName, cloudEntity, serializer));
                }
                else
                {
                    try
                    {
                        fatEntity.SetData(stream.ToArray());
                    }
                    catch (ArgumentException)
                    {
                        fatEntity.SetData(PutOverflowingFatEntityAndWrap(tableName, cloudEntity, serializer));
                    }
                }
            }
            return fatEntity;
        }

        private byte[] PutOverflowingFatEntityAndWrap<T>(string tableName, CloudEntity<T> entity, IDataSerializer serializer)
        {
            var blobRef = new OverflowingFatEntityBlobName<T>(tableName, entity.PartitionKey, entity.RowKey);

            // HACK: In this case serialization is performed another time (internally)
            _blobStorage.PutBlob(blobRef, entity.Value);

            var mw = new OverflowWrapper
            {
                ContainerName = blobRef.ContainerName,
                BlobName = blobRef.ToString()
            };

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(mw, stream, typeof(OverflowWrapper));
                var serializerWrapper = stream.ToArray();
                
                return serializerWrapper;
            }
        }

        private CloudEntity<T> DeserializeFatEntity<T>(FatEntity fatEntity, IDataSerializer serializer, string etag)
        {
            // TODO: Refactor this method away, and integrate it with the GetInternal method. Will give better flexibility to handle multiple entities.

            T value;
            var bytes = fatEntity.GetData();

            var entityAsT = serializer.TryDeserializeAs<T>(bytes);

            if (entityAsT.IsSuccess)
                value = entityAsT.Value;
            else
            {
                var entityAsWrapper = serializer.TryDeserializeAs<OverflowWrapper>(bytes);
                if (entityAsWrapper.IsSuccess)
                {
                    var overflow = entityAsWrapper.Value;
                    var blobContent = _blobStorage.GetBlob<T>(overflow.ContainerName, overflow.BlobName);
                    if (blobContent.HasValue)
                        value = blobContent.Value;
                    else
                        // TODO: Pick a nicer exception type
                        throw new Exception("Found an overflow wrapper, but failed to get the referenced blob");
                }
                else
                    throw new DataCorruptionException(string.Format("Unable to deserialize FatEntity to either {0} or OverflowWrapper", typeof(T).Name), entityAsWrapper.Error);
            }

            // TODO: potentially should delete the table entry if we've failed, so no one else attempts to get this bad entry.

            return new CloudEntity<T>
            {
                PartitionKey = fatEntity.PartitionKey,
                RowKey = fatEntity.RowKey,
                Timestamp = fatEntity.Timestamp,
                ETag = etag,
                Value = value
            };
        }

        static System.Tuple<string, string> ToId<T>(CloudEntity<T> entity)
        {
            return System.Tuple.Create(entity.PartitionKey, entity.RowKey);
        }
        static System.Tuple<string, string> ToId(MockTableEntry entry)
        {
            return System.Tuple.Create(entry.PartitionKey, entry.RowKey);
        }

        class MockTableEntry
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }
            public string ETag { get; set; }
            public FatEntity Value { get; set; }
        }
    }
}
