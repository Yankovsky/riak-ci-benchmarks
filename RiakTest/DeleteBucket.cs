using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class DeleteBucket : RiakTestBase
    {
        private readonly int _count;

        public DeleteBucket(string bucket, int count) : base(bucket, false, false)
        {
            _count = count;
        }

        protected override void SetUp()
        {
            // nothing to do here
        }

        protected override void DoWork()
        {
            var deleteBucketMethods = new List<DeleteBucketMethod>
                {
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.DeleteBucket method",
                            Method = () => RiakClient.DeleteBucket(Bucket)
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by RiakClient.ListKeys(Bucket)",
                            Method = () =>
                                {
                                    foreach (var key in RiakClient.ListKeys(Bucket).Value)
                                    {
                                        RiakClient.Delete(Bucket, key);
                                    }
                                }
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by $bucket index",
                            Method = () =>
                                {
                                    foreach (var key in RiakClient.IndexGet(Bucket, "$bucket", Bucket).Value)
                                    {
                                        RiakClient.Delete(Bucket, key);
                                    }
                                }
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by $bucket index using RiakClient.Batch",
                            Method = () => RiakClient.Batch(x =>
                                {
                                    foreach (var key in x.IndexGet(Bucket, "$bucket", Bucket).Value)
                                    {
                                        x.Delete(Bucket, key);
                                    }
                                })
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by $bucket index using RiakClient.Delete(IEnumerable<RiakObjectId>)",
                            Method = () => RiakClient.Delete(RiakClient.IndexGet(Bucket, "$bucket", Bucket).Value.Select(key => new RiakObjectId(Bucket, key)))
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by $key index on range minChar to maxChar",
                            Method = () =>
                                {
                                    var minChar = char.MinValue.ToString(CultureInfo.InvariantCulture);
                                    var maxChar = char.MaxValue.ToString(CultureInfo.InvariantCulture);
                                    foreach (var key in RiakClient.IndexGet(Bucket, "$key", minChar, maxChar).Value)
                                    {
                                        RiakClient.Delete(Bucket, key);
                                    }
                                }
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by $key index on range minChar to maxChar using RiakClient.Batch",
                            Method = () => RiakClient.Batch(x =>
                                {
                                    var minChar = char.MinValue.ToString(CultureInfo.InvariantCulture);
                                    var maxChar = char.MaxValue.ToString(CultureInfo.InvariantCulture);
                                    foreach (var key in x.IndexGet(Bucket, "$key", minChar, maxChar).Value)
                                    {
                                        x.Delete(Bucket, key);
                                    }
                                })
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by $key index on range minChar to maxChar using RiakClient.Delete(IEnumerable<RiakObjectId>)",
                            Method = () =>
                                {
                                    var minChar = char.MinValue.ToString(CultureInfo.InvariantCulture);
                                    var maxChar = char.MaxValue.ToString(CultureInfo.InvariantCulture);
                                    RiakClient.Delete(RiakClient.IndexGet(Bucket, "$key", minChar, maxChar).Value.Select(key => new RiakObjectId(Bucket, key)));
                                }
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by RiakClient.ListKeysFromIndex(Bucket)",
                            Method = () =>
                                {
                                    foreach (var key in RiakClient.ListKeysFromIndex(Bucket).Value)
                                    {
                                        RiakClient.Delete(Bucket, key);
                                    }
                                }
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by RiakClient.ListKeysFromIndex(Bucket) using RiakClient.Batch",
                            Method = () => RiakClient.Batch(x =>
                                {
                                    foreach (var key in x.ListKeysFromIndex(Bucket).Value)
                                    {
                                        x.Delete(Bucket, key);
                                    }
                                })
                        },
                    new DeleteBucketMethod
                        {
                            Name = "RiakClient.Delete on keys obtained by RiakClient.ListKeysFromIndex(Bucket) using RiakClient.Delete(IEnumerable<RiakObjectId>)",
                            Method = () => RiakClient.Delete(RiakClient.ListKeysFromIndex(Bucket).Value.Select(key => new RiakObjectId(Bucket, key)))
                        }
                };

            foreach (var deleteBucketMethod in deleteBucketMethods)
            {
                RiakClient.Put(DataGenerator.GenerateGoodsComments(_count).Select(x => new RiakObject(Bucket, x.Id.ToString(CultureInfo.InvariantCulture), x)));
                Bench(deleteBucketMethod.Name, deleteBucketMethod.Method);
            }
        }
    }

    internal class DeleteBucketMethod
    {
        public string Name { get; set; }
        public Action Method { get; set; }
    }
}