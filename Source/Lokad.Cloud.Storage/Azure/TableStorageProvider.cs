#region Copyright (c) Lokad 2010-2011
// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Web;
using Lokad.Cloud.Storage.Instrumentation;
using Lokad.Cloud.Storage.Instrumentation.Events;
using Microsoft.WindowsAzure.StorageClient;
using System.IO;

namespace Lokad.Cloud.Storage.Azure
{
    /// <summary>Implementation based on the Table Storage of Windows Azure.</summary>
    public class TableStorageProvider : ITableStorageProvider
    {
        // HACK: those tokens will probably be provided as constants in the StorageClient library
        const int MaxEntityTransactionCount = 100;

        // HACK: Lowering the maximal payload, to avoid corner cases #117 (ContentLengthExceeded)
        // [vermorel] 128kB is purely arbitrary, only taken as a reasonable safety margin
        const int MaxEntityTransactionPayload = 4 * 1024 * 1024 - 128 * 1024; // 4 MB - 128kB

        const string ContinuationNextRowKeyToken = "x-ms-continuation-NextRowKey";
        const string ContinuationNextPartitionKeyToken = "x-ms-continuation-NextPartitionKey";
        const string NextRowKeyToken = "NextRowKey";
        const string NextPartitionKeyToken = "NextPartitionKey";

        public const string OverflowingFatEntityContainerName = "lokad-cloud-overflowing-fatentities";

        readonly CloudTableClient _tableStorage;
        readonly IDataSerializer _serializer;
        readonly IStorageObserver _observer;
        readonly RetryPolicies _policies;
        readonly IBlobStorageProvider _blobStorage;

        /// <summary>IoC constructor.</summary>
        /// <param name="observer">Can be <see langword="null"/>.</param>
        public TableStorageProvider(CloudTableClient tableStorage, IBlobStorageProvider blobStorage, IDataSerializer serializer, IStorageObserver observer = null)
        {
            _policies = new RetryPolicies(observer);
            _tableStorage = tableStorage;
            _blobStorage = blobStorage;
            _serializer = serializer;
            _observer = observer;
        }

        /// <remarks></remarks>
        public bool CreateTable(string tableName)
        {
            var flag = false;
            Retry.Do(_policies.SlowInstantiation, CancellationToken.None, () => flag = _tableStorage.CreateTableIfNotExist(tableName));

            return flag;
        }

        /// <remarks></remarks>
        public bool DeleteTable(string tableName)
        {
            var flag = false;
            Retry.Do(_policies.SlowInstantiation, CancellationToken.None, () => flag = _tableStorage.DeleteTableIfExist(tableName));

            _blobStorage.DeleteAllBlobs(OverflowingFatEntityContainerName, tableName);

            return flag;
        }

        /// <remarks></remarks>
        public IEnumerable<string> GetTables()
        {
            return _tableStorage.ListTables();
        }

        /// <remarks></remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName)
        {
            if(null == tableName) throw new ArgumentNullException("tableName");

            var context = _tableStorage.GetDataServiceContext();
            return GetInternal<T>(context, tableName, Maybe<string>.Empty);
        }

        /// <remarks></remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey)
        {
            if(null == tableName) throw new ArgumentNullException("tableName");
            if(null == partitionKey) throw new ArgumentNullException("partitionKey");
            if (partitionKey.Contains("'"))
                throw new ArgumentOutOfRangeException("partitionKey", "Incorrect char in partitionKey.");

            var filter = string.Format("(PartitionKey eq '{0}')", HttpUtility.UrlEncode(partitionKey));

            var context = _tableStorage.GetDataServiceContext();
            return GetInternal<T>(context, tableName, filter);
        }

        /// <remarks></remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey, string startRowKey, string endRowKey)
        {
            if(null == tableName) throw new ArgumentNullException("tableName");
            if(null == partitionKey) throw new ArgumentNullException("partitionKey");
            if (partitionKey.Contains("'"))
                throw new ArgumentOutOfRangeException("partitionKey", "Incorrect char.");
            if(startRowKey != null && startRowKey.Contains("'"))
                throw new ArgumentOutOfRangeException("startRowKey", "Incorrect char.");
            if(endRowKey != null && endRowKey.Contains("'"))
                throw new ArgumentOutOfRangeException("endRowKey", "Incorrect char.");

            var filter = string.Format("(PartitionKey eq '{0}')", HttpUtility.UrlEncode(partitionKey));

            // optional starting range constraint
            if (!string.IsNullOrEmpty(startRowKey))
            {
                // ge = GreaterThanOrEqual (inclusive)
                filter += string.Format(" and (RowKey ge '{0}')", HttpUtility.UrlEncode(startRowKey));
            }

            if (!string.IsNullOrEmpty(endRowKey))
            {
                // lt = LessThan (exclusive)
                filter += string.Format(" and (RowKey lt '{0}')", HttpUtility.UrlEncode(endRowKey));
            }

            var context = _tableStorage.GetDataServiceContext();
            return GetInternal<T>(context, tableName, filter);
        }

        /// <remarks></remarks>
        public IEnumerable<CloudEntity<T>> Get<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            if(null == tableName) throw new ArgumentNullException("tableName");
            if(null == partitionKey) throw new ArgumentNullException("partitionKey");
            if(partitionKey.Contains("'")) throw new ArgumentOutOfRangeException("partitionKey", "Incorrect char.");

            var context = _tableStorage.GetDataServiceContext();

            foreach (var slice in Slice(rowKeys, MaxEntityTransactionCount))
            {
                // work-around the limitation of ADO.NET that does not provide a native way
                // of query a set of specified entities directly.
                var builder = new StringBuilder();
                builder.Append(string.Format("(PartitionKey eq '{0}') and (", HttpUtility.UrlEncode(partitionKey)));
                for (int i = 0; i < slice.Length; i++)
                {
                    // in order to avoid SQL-injection-like problems 
                    if (slice[i].Contains("'")) throw new ArgumentOutOfRangeException("rowKeys", "Incorrect char.");

                    builder.Append(string.Format("(RowKey eq '{0}')", HttpUtility.UrlEncode(slice[i])));
                    if (i < slice.Length - 1)
                    {
                        builder.Append(" or ");
                    }
                }
                builder.Append(")");

                foreach(var entity in GetInternal<T>(context, tableName, builder.ToString()))
                {
                    yield return entity;
                }
            }
        }

        /// <remarks></remarks>
        private IEnumerable<CloudEntity<T>> GetInternal<T>(TableServiceContext context, string tableName, Maybe<string> filter)
        {
            string continuationRowKey = null;
            string continuationPartitionKey = null;

            var stopwatch = Stopwatch.StartNew();

            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            do
            {
                var query = context.CreateQuery<FatEntity>(tableName);

                if (filter.HasValue)
                {
                    query = query.AddQueryOption("$filter", filter.Value);
                }

                if (null != continuationRowKey)
                {
                    query = query.AddQueryOption(NextRowKeyToken, continuationRowKey)
                        .AddQueryOption(NextPartitionKeyToken, continuationPartitionKey);
                }

                QueryOperationResponse response = null;
                FatEntity[] fatEntities = null;

                Retry.Do(_policies.TransientTableErrorBackOff, CancellationToken.None, () =>
                    {
                        try
                        {
                            response = query.Execute() as QueryOperationResponse;
                            fatEntities = ((IEnumerable<FatEntity>)response).ToArray();
                        }
                        catch (DataServiceQueryException ex)
                        {
                            // if the table does not exist, there is nothing to return
                            var errorCode = RetryPolicies.GetErrorCode(ex);
                            if (TableErrorCodeStrings.TableNotFound == errorCode
                                || StorageErrorCodeStrings.ResourceNotFound == errorCode)
                            {
                                fatEntities = new FatEntity[0];
                                return;
                            }

                            throw;
                        }
                    });

                NotifySucceeded(StorageOperationType.TableQuery, stopwatch);

                foreach (var fatEntity in fatEntities)
                {
                    var etag = context.Entities.First(e => e.Entity == fatEntity).ETag;
                    context.Detach(fatEntity);
                    yield return DeserializeFatEntity<T>(fatEntity, _serializer, etag);
                }

                Debug.Assert(context.Entities.Count == 0);

                if (null != response && response.Headers.ContainsKey(ContinuationNextRowKeyToken))
                {
                    continuationRowKey = response.Headers[ContinuationNextRowKeyToken];
                    continuationPartitionKey = response.Headers[ContinuationNextPartitionKeyToken];

                    stopwatch.Restart();
                }
                else
                {
                    continuationRowKey = null;
                    continuationPartitionKey = null;
                }

            } while (null != continuationRowKey);
        }

        /// <remarks></remarks>
        public void Insert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                InsertInternal(tableName, g);
            }
        }

        /// <remarks></remarks>
        void InsertInternal<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            var context = _tableStorage.GetDataServiceContext();
            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            var stopwatch = new Stopwatch();

            var fatEntities = entities.Select(e => Tuple.Create(SerializeFatEntity(tableName, e, _serializer), e));

            var noBatchMode = false;

            foreach (var slice in SliceEntities(fatEntities, e => e.Item1.GetPayload()))
            {
                stopwatch.Restart();

                var cloudEntityOfFatEntity = new Dictionary<object, CloudEntity<T>>();
                foreach (var fatEntity in slice)
                {
                    context.AddObject(tableName, fatEntity.Item1);
                    cloudEntityOfFatEntity.Add(fatEntity.Item1, fatEntity.Item2);
                }

                Retry.Do(_policies.TransientTableErrorBackOff, CancellationToken.None, () =>
                    {
                        try
                        {
                            // HACK: nested try/catch
                            try
                            {
                                context.SaveChanges(noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                                ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                            }
                            // special casing the need for table instantiation
                            catch (DataServiceRequestException ex)
                            {
                                var errorCode = RetryPolicies.GetErrorCode(ex);
                                if (errorCode == TableErrorCodeStrings.TableNotFound
                                    || errorCode == StorageErrorCodeStrings.ResourceNotFound)
                                {
                                    Retry.Do(_policies.SlowInstantiation, CancellationToken.None, () =>
                                        {
                                            try
                                            {
                                                _tableStorage.CreateTableIfNotExist(tableName);
                                            }
                                            // HACK: incorrect behavior of the StorageClient (2010-09)
                                            // Fails to behave properly in multi-threaded situations
                                            catch (StorageClientException cex)
                                            {
                                                if (cex.ExtendedErrorInformation == null
                                                    || cex.ExtendedErrorInformation.ErrorCode != TableErrorCodeStrings.TableAlreadyExists)
                                                {
                                                    throw;
                                                }
                                            }
                                            context.SaveChanges(noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                                            ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                        });
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }
                        catch (DataServiceRequestException ex)
                        {
                            var errorCode = RetryPolicies.GetErrorCode(ex);

                            if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                            {
                                // if batch does not work, then split into elementary requests
                                // PERF: it would be better to split the request in two and retry
                                context.SaveChanges();
                                ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                noBatchMode = true;
                            }
                            // HACK: undocumented code returned by the Table Storage
                            else if (errorCode == "ContentLengthExceeded")
                            {
                                context.SaveChanges();
                                ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                noBatchMode = true;
                            }
                            else
                            {
                                throw;
                            }
                        }
                        catch (DataServiceQueryException ex)
                        {
                            // HACK: code duplicated

                            var errorCode = RetryPolicies.GetErrorCode(ex);

                            if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                            {
                                // if batch does not work, then split into elementary requests
                                // PERF: it would be better to split the request in two and retry
                                context.SaveChanges();
                                ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                noBatchMode = true;
                            }
                            else
                            {
                                throw;
                            }
                        }
                    });

                NotifySucceeded(StorageOperationType.TableInsert, stopwatch);
            }
        }

        /// <remarks></remarks>
        public void Update<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                UpdateInternal(tableName, g, force);
            }
        }

        /// <remarks></remarks>
        void UpdateInternal<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            var context = _tableStorage.GetDataServiceContext();
            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            var stopwatch = new Stopwatch();

            var fatEntities = entities.Select(e => Tuple.Create(SerializeFatEntity(tableName, e, _serializer), e));

            var noBatchMode = false;

            foreach (var slice in SliceEntities(fatEntities, e => e.Item1.GetPayload()))
            {
                stopwatch.Restart();

                var cloudEntityOfFatEntity = new Dictionary<object, CloudEntity<T>>();
                foreach (var fatEntity in slice)
                {
                    // entities should be updated in a single round-trip
                    context.AttachTo(tableName, fatEntity.Item1, MapETag(fatEntity.Item2.ETag, force));
                    context.UpdateObject(fatEntity.Item1);
                    cloudEntityOfFatEntity.Add(fatEntity.Item1, fatEntity.Item2);
                }

                Retry.Do(_policies.TransientTableErrorBackOff, CancellationToken.None, () =>
                    {
                        try
                        {
                            context.SaveChanges(noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                            ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                        }
                        catch (DataServiceRequestException ex)
                        {
                            var errorCode = RetryPolicies.GetErrorCode(ex);

                            if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                            {
                                // if batch does not work, then split into elementary requests
                                // PERF: it would be better to split the request in two and retry
                                context.SaveChanges();
                                ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                noBatchMode = true;
                            }
                            else if (errorCode == TableErrorCodeStrings.TableNotFound)
                            {
                                Retry.Do(_policies.SlowInstantiation, CancellationToken.None, () =>
                                    {
                                        try
                                        {
                                            _tableStorage.CreateTableIfNotExist(tableName);
                                        }
                                        // HACK: incorrect behavior of the StorageClient (2010-09)
                                        // Fails to behave properly in multi-threaded situations
                                        catch (StorageClientException cex)
                                        {
                                            if (cex.ExtendedErrorInformation.ErrorCode != TableErrorCodeStrings.TableAlreadyExists)
                                            {
                                                throw;
                                            }
                                        }
                                        context.SaveChanges(noBatchMode ? SaveChangesOptions.None : SaveChangesOptions.Batch);
                                        ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                    });
                            }
                            else if (errorCode == StorageErrorCodeStrings.ResourceNotFound)
                            {
                                throw new InvalidOperationException("Cannot call update on a resource that does not exist", ex);
                            }
                            else
                            {
                                throw;
                            }
                        }
                        catch (DataServiceQueryException ex)
                        {
                            // HACK: code duplicated

                            var errorCode = RetryPolicies.GetErrorCode(ex);

                            if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                            {
                                // if batch does not work, then split into elementary requests
                                // PERF: it would be better to split the request in two and retry
                                context.SaveChanges();
                                ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                                noBatchMode = true;
                            }
                            else
                            {
                                throw;
                            }
                        }
                    });

                NotifySucceeded(StorageOperationType.TableUpdate, stopwatch);
            }
        }

        /// <remarks></remarks>
        public void Upsert<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                UpsertInternal(tableName, g);
            }
        }

        /// <remarks>Upsert is making several storage calls to emulate the 
        /// missing semantic from the Table Storage.</remarks>
        void UpsertInternal<T>(string tableName, IEnumerable<CloudEntity<T>> entities)
        {
            if (_tableStorage.BaseUri.Host == "127.0.0.1")
            {
                // HACK: Dev Storage of v1.6 tools does NOT support Upsert yet -> emulate

                // checking for entities that already exist
                var partitionKey = entities.First().PartitionKey;
                var existingKeys = new HashSet<string>(
                Get<T>(tableName, partitionKey, entities.Select(e => e.RowKey)).Select(e => e.RowKey));

                // inserting or updating depending on the presence of the keys
                Insert(tableName, entities.Where(e => !existingKeys.Contains(e.RowKey)));
                Update(tableName, entities.Where(e => existingKeys.Contains(e.RowKey)), true);

                return;
            }

            var context = _tableStorage.GetDataServiceContext();
            context.MergeOption = MergeOption.AppendOnly;
            context.ResolveType = ResolveFatEntityType;

            var stopwatch = new Stopwatch();

            var fatEntities = entities.Select(e => Tuple.Create(SerializeFatEntity(tableName, e, _serializer), e));

            var noBatchMode = false;

            foreach (var slice in SliceEntities(fatEntities, e => e.Item1.GetPayload()))
            {
                stopwatch.Restart();

                var cloudEntityOfFatEntity = new Dictionary<object, CloudEntity<T>>();
                foreach (var fatEntity in slice)
                {
                    // entities should be updated in a single round-trip
                    context.AttachTo(tableName, fatEntity.Item1);
                    context.UpdateObject(fatEntity.Item1);
                    cloudEntityOfFatEntity.Add(fatEntity.Item1, fatEntity.Item2);
                }

                Retry.Do(_policies.TransientTableErrorBackOff, CancellationToken.None, () =>
                {
                    try
                    {
                        context.SaveChanges(noBatchMode ? SaveChangesOptions.ReplaceOnUpdate : SaveChangesOptions.ReplaceOnUpdate | SaveChangesOptions.Batch);
                        ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                    }
                    catch (DataServiceRequestException ex)
                    {
                        var errorCode = RetryPolicies.GetErrorCode(ex);

                        if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                        {
                            // if batch does not work, then split into elementary requests
                            // PERF: it would be better to split the request in two and retry
                            context.SaveChanges(SaveChangesOptions.ReplaceOnUpdate);
                            ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                            noBatchMode = true;
                        }
                        else if (errorCode == TableErrorCodeStrings.TableNotFound)
                        {
                            Retry.Do(_policies.SlowInstantiation, CancellationToken.None, () =>
                            {
                                try
                                {
                                    _tableStorage.CreateTableIfNotExist(tableName);
                                }
                                // HACK: incorrect behavior of the StorageClient (2010-09)
                                // Fails to behave properly in multi-threaded situations
                                catch (StorageClientException cex)
                                {
                                    if (cex.ExtendedErrorInformation.ErrorCode != TableErrorCodeStrings.TableAlreadyExists)
                                    {
                                        throw;
                                    }
                                }
                                context.SaveChanges(noBatchMode ? SaveChangesOptions.ReplaceOnUpdate : SaveChangesOptions.ReplaceOnUpdate | SaveChangesOptions.Batch);
                                ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                            });
                        }
                        else if (errorCode == StorageErrorCodeStrings.ResourceNotFound)
                        {
                            throw new InvalidOperationException("Cannot call update on a resource that does not exist", ex);
                        }
                        else
                        {
                            throw;
                        }
                    }
                    catch (DataServiceQueryException ex)
                    {
                        // HACK: code duplicated

                        var errorCode = RetryPolicies.GetErrorCode(ex);

                        if (errorCode == StorageErrorCodeStrings.OperationTimedOut)
                        {
                            // if batch does not work, then split into elementary requests
                            // PERF: it would be better to split the request in two and retry
                            context.SaveChanges(SaveChangesOptions.ReplaceOnUpdate);
                            ReadETagsAndDetach(context, (entity, etag) => cloudEntityOfFatEntity[entity].ETag = etag);
                            noBatchMode = true;
                        }
                        else
                        {
                            throw;
                        }
                    }
                });

                NotifySucceeded(StorageOperationType.TableUpsert, stopwatch);
            }
        }

        /// <summary>Slice entities according the payload limitation of
        /// the transaction group, plus the maximal number of entities to
        /// be embedded into a single transaction.</summary>
        static IEnumerable<T[]> SliceEntities<T>(IEnumerable<T> entities, Func<T, int> getPayload)
        {
            var accumulator = new List<T>(100);
            var payload = 0;
            foreach (var entity in entities)
            {
                var entityPayLoad = getPayload(entity);

                if (accumulator.Count >= MaxEntityTransactionCount ||
                    payload + entityPayLoad >= MaxEntityTransactionPayload)
                {
                    yield return accumulator.ToArray();
                    accumulator.Clear();
                    payload = 0;
                }

                accumulator.Add(entity);
                payload += entityPayLoad;
            }

            if (accumulator.Count > 0)
            {
                yield return accumulator.ToArray();
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
            var stopwatch = Stopwatch.StartNew();

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

                NotifySucceeded(StorageOperationType.TableWrap, stopwatch);

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

        /// <remarks></remarks>
        public void Delete<T>(string tableName, string partitionKey, IEnumerable<string> rowKeys)
        {
            DeleteInternal<T>(tableName, partitionKey, rowKeys.Select(k => Tuple.Create(k, "*")), true);
        }

        /// <remarks></remarks>
        public void Delete<T>(string tableName, IEnumerable<CloudEntity<T>> entities, bool force)
        {
            foreach (var g in entities.GroupBy(e => e.PartitionKey))
            {
                DeleteInternal<T>(tableName, 
                    g.Key, g.Select(e => Tuple.Create(e.RowKey, MapETag(e.ETag, force))), force);
            }
        }

        /// <remarks></remarks>
        void DeleteInternal<T>(string tableName, string partitionKey, IEnumerable<Tuple<string,string>> rowKeysAndETags, bool force)
        {
            var context = _tableStorage.GetDataServiceContext();

            var stopwatch = new Stopwatch();

            // CAUTION: make sure to get rid of potential duplicate in rowkeys.
            // (otherwise insertion in 'context' is likely to fail)
            foreach (var s in Slice(rowKeysAndETags
                                    // Similar effect than 'Distinct' based on 'RowKey'
                                    .ToLookup(p => p.Item1, p => p).Select(g => g.First()), 
                                    MaxEntityTransactionCount))
            {
                stopwatch.Restart();

                var slice = s;

                DeletionStart: // 'slice' might have been refreshed if some entities were already deleted

                foreach (var rowKeyAndETag in slice)
                {
                    // Deleting entities in 1 roundtrip
                    // http://blog.smarx.com/posts/deleting-entities-from-windows-azure-without-querying-first
                    var mock = new FatEntity
                        {
                            PartitionKey = partitionKey,
                            RowKey = rowKeyAndETag.Item1
                        };

                    context.AttachTo(tableName, mock, rowKeyAndETag.Item2);
                    context.DeleteObject(mock);

                }

                try // HACK: [vermorel] if a single entity is missing, then the whole batch operation is aborded
                {

                    try // HACK: nested try/catch to handle the special case where the table is missing
                    {
                        Retry.Do(_policies.TransientTableErrorBackOff, CancellationToken.None, () => context.SaveChanges(SaveChangesOptions.Batch));
                    }
                    catch (DataServiceRequestException ex)
                    {
                        // if the table is missing, no need to go on with the deletion
                        var errorCode = RetryPolicies.GetErrorCode(ex);
                        if (TableErrorCodeStrings.TableNotFound == errorCode)
                        {
                            NotifySucceeded(StorageOperationType.TableDelete, stopwatch);
                            return;
                        }

                        throw;
                    }
                }
                    // if some entities exist
                catch (DataServiceRequestException ex)
                {
                    var errorCode = RetryPolicies.GetErrorCode(ex);

                    // HACK: Table Storage both implement a bizarre non-idempotent semantic
                    // but in addition, it throws a non-documented exception as well. 
                    if (errorCode != "ResourceNotFound")
                    {
                        throw;
                    }

                    slice = Get<T>(tableName, partitionKey, slice.Select(p => p.Item1))
                        .Select(e => Tuple.Create(e.RowKey, MapETag(e.ETag, force))).ToArray();

                    // entities with same name will be added again
                    context = _tableStorage.GetDataServiceContext();

                    // HACK: [vermorel] yes, gotos are horrid, but other solutions are worst here.
                    goto DeletionStart;
                }

                NotifySucceeded(StorageOperationType.TableDelete, stopwatch);

                // TODO: clean up overflowed entities. No major harm keeping them though, beyond paying for their storage.
                // Potential contention issues if deleting when another instance is inserting with the same id. Needs some thought...
            }
        }

        static Type ResolveFatEntityType(string name)
        {
            return typeof (FatEntity);
        }

        static string MapETag(string etag, bool force)
        {
            return force || string.IsNullOrEmpty(etag)
                ? "*"
                : etag;
        }

        static void ReadETagsAndDetach(DataServiceContext context, Action<object, string> write)
        {
            foreach (var entity in context.Entities)
            {
                write(entity.Entity, entity.ETag);
                context.Detach(entity.Entity);
            }
        }

        /// <summary>
        /// Performs lazy splitting of the provided collection into collections of <paramref name="sliceLength"/>
        /// </summary>
        /// <typeparam name="TItem">The type of the item.</typeparam>
        /// <param name="source">The source.</param>
        /// <param name="sliceLength">Maximum length of the slice.</param>
        /// <returns>lazy enumerator of the collection of arrays</returns>
        public static IEnumerable<TItem[]> Slice<TItem>(IEnumerable<TItem> source, int sliceLength)
        {
            if (source == null) throw new ArgumentNullException("source");
            if (sliceLength <= 0)
                throw new ArgumentOutOfRangeException("sliceLength", "value must be greater than 0");

            var list = new List<TItem>(sliceLength);
            foreach (var item in source)
            {
                list.Add(item);
                if (sliceLength == list.Count)
                {
                    yield return list.ToArray();
                    list.Clear();
                }
            }

            if (list.Count > 0)
                yield return list.ToArray();
        }

        private void NotifySucceeded(StorageOperationType operationType, Stopwatch stopwatch)
        {
            if (_observer != null)
            {
                _observer.Notify(new StorageOperationSucceededEvent(operationType, stopwatch.Elapsed));
            }
        }
    }

    internal class OverflowingFatEntityBlobName<T> : BlobName<T>
    {
        public override string ContainerName
        {
            get { return TableStorageProvider.OverflowingFatEntityContainerName; }
        }

        /// <summary>Indicates the name of the table where the fat entity has been originally pushed.</summary>
        [Rank(0)]
        public string TableName;

        /// <summary>Partition key of the original fat entitiy</summary>
        [Rank(1)]
        public string PartitionKey;

        /// <summary>Row Key of the original fat entity</summary>
        [Rank(2)]
        public string RowKey;

        internal OverflowingFatEntityBlobName(string tableName, string partitionKey, string rowKey)
        {
            TableName = tableName;
            PartitionKey = partitionKey;
            RowKey = rowKey;
        }
    }
}