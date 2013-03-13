using System;
using System.Collections.Generic;
using System.Diagnostics;
using CorrugatedIron;
using CorrugatedIron.Models;

namespace RiakTest
{
    internal class BenchConfiguration
    {
        public string Backend { get; set; }

        public uint RWN { get; set; }
    }

    internal class BenchTest : RiakTest
    {
        private const string SampleMemoryBucket = "sample-memory-bucket";
        private const string SampleBitcaskBucket = "sample-bitcask-bucket";
        private const string SampleLevelDbBucket = "sample-leveldb-bucket";
        private const int N = 1000;

        private static void Write(IRiakClient riakClient, string bucket)
        {
            for (var i = 0; i < N; i++)
            {
                riakClient.Put(new RiakObject(bucket, i.ToString(), i));
            }
        }

        private static void Read(IRiakClient riakClient, string bucket)
        {
            for (var i = 0; i < N; i++)
            {
                var v = riakClient.Get(bucket, i.ToString());
                if (v.Value.GetObject<int>() != i)
                {
                    throw new SystemException();
                }
            }
        }

        private static void Bench(Action<IRiakClient, string> func, IRiakClient riakClient, string memoryBackend, string sampleMemoryBackendBucket)
        {
            Console.WriteLine(memoryBackend);
            Console.WriteLine(func.Method.Name + " " + N + " objects");
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            func(riakClient, sampleMemoryBackendBucket);
            stopWatch.Stop();
            Console.WriteLine(stopWatch.ElapsedMilliseconds);
        }

        protected override void SetUp()
        {
            throw new NotImplementedException();
        }

        protected override void DoWork()
        {
            var riakCluster = RiakCluster.FromConfig("riakConfig");
            var riakClient = riakCluster.CreateClient();

            var benchConfigurations = new List<Tuple<string, string, BenchConfiguration>>
                {
                    new Tuple<string, string, BenchConfiguration>(SampleMemoryBucket, "Memory backend", new BenchConfiguration {Backend = "memory", RWN = 1}),
                    new Tuple<string, string, BenchConfiguration>(SampleBitcaskBucket, "Bitcask backend", new BenchConfiguration {Backend = "bitcask", RWN = 1}),
                    new Tuple<string, string, BenchConfiguration>(SampleLevelDbBucket, "LevelDB backend", new BenchConfiguration {Backend = "eleveldb", RWN = 1})
                };

            Console.WriteLine("Ping " + riakClient.Ping().IsSuccess);

            try
            {
                foreach (var benchConfiguration in benchConfigurations)
                {
                    var b = benchConfiguration.Item1;
                    var n = benchConfiguration.Item2;
                    var bc = benchConfiguration.Item3;
                    riakClient.SetBucketProperties(b, new RiakBucketProperties().SetBackend(bc.Backend).SetNVal(bc.RWN).SetRVal(bc.RWN).SetWVal(bc.RWN));
                    Bench(Write, riakClient, n, b);
                    Bench(Read, riakClient, n, b);
                }
            }
            finally
            {
                foreach (var benchConfiguration in benchConfigurations)
                {
                    //riakClient.DeleteBucket(benchConfiguration.Item1);
                }
            }

            Console.WriteLine("Press enter to continue...");
            Console.Read();
        }

        protected override void TearDown()
        {
            throw new NotImplementedException();
        }
    }
}