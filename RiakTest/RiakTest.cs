using System;
using System.Diagnostics;
using System.Reflection;
using CorrugatedIron;

namespace RiakTest
{
    public abstract class RiakTest
    {
        protected readonly IRiakClient RiakClient;

        protected RiakTest()
        {
            var riakCluster = RiakCluster.FromConfig("riakConfig");
            RiakClient = riakCluster.CreateClient();
        }

        public void Run(bool setUp = true, bool tearDown = true)
        {
            try
            {
                if (setUp)
                {
                    SetUp();
                }
                DoWork();
            }
            finally
            {
                if (tearDown)
                {
                    TearDown();
                }
            }
        }

        protected abstract void SetUp();
        protected abstract void DoWork();
        protected abstract void TearDown();

        protected void Bench(string testName, Action<string> operation, string bucket)
        {
            Console.WriteLine("{0}\n{1}\nBucket: {2}", MethodBase.GetCurrentMethod().Name, testName, bucket);
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            operation(bucket);
            stopWatch.Stop();
            Console.WriteLine("Elapsed time (ms): {0}", stopWatch.ElapsedMilliseconds);
        }
    }
}