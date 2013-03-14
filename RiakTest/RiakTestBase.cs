using System;
using System.Diagnostics;
using CorrugatedIron;

namespace RiakTest
{
    public abstract class RiakTestBase
    {
        private readonly bool _setUp;
        private readonly bool _tearDown;

        protected RiakTestBase(string bucket, bool setUp = true, bool tearDown = true)
        {
            Bucket = bucket;

            _setUp = setUp;
            _tearDown = tearDown;

            var riakCluster = RiakCluster.FromConfig("riakConfig");
            RiakClient = riakCluster.CreateClient();
        }

        protected IRiakClient RiakClient { get; private set; }
        protected string Bucket { get; private set; }

        protected abstract void SetUp();
        protected abstract void DoWork();

        protected void TearDown()
        {
            foreach (var key in RiakClient.ListKeysFromIndex(Bucket).Value) { RiakClient.Delete(Bucket, key); }
        }

        public void Run()
        {
            try
            {
                if (_setUp)
                {
                    SetUp();
                }
                DoWork();
            }
            finally
            {
                if (_tearDown)
                {
                    TearDown();
                }
            }
        }

        protected void Bench(string benchName, Action operation)
        {
            Console.WriteLine("{0}", benchName);
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            operation();
            stopWatch.Stop();
            Console.WriteLine("Elapsed time (ms): {0}", stopWatch.ElapsedMilliseconds);
        }
    }
}