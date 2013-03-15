using System;
using System.Linq;
using CorrugatedIron.Models;
using CorrugatedIron.Models.MapReduce;
using Newtonsoft.Json;

namespace RiakTest
{
    /// <summary>
    /// There are problems with big integers in javascript. We can loose values and cannot rely on integer indexes in javascript map reduce phases.
    /// http://blog.8thlight.com/myles-megyesi/2013/01/18/riak-mapreduce.html
    /// http://ecma262-5.com/ELS5_HTML.htm#Section_8.5
    /// </summary>
    public class BigIntegersJavascriptTest : RiakTestBase
    {
        private long _maxLong = long.MaxValue;
        private string key = "key-of-big-int-obj";

        public BigIntegersJavascriptTest(string bucket) : base(bucket)
        {
        }

        protected override void SetUp()
        {
            RiakClient.Put(new RiakObject(Bucket, key, _maxLong));
        }

        protected override void DoWork()
        {
            var getByKeyResult = RiakClient.Get(Bucket, key).Value.GetObject<long>();
            Console.WriteLine("Is written long.MaxValue equals to obtained by key value? {0}", _maxLong == getByKeyResult);

            Console.WriteLine("Now try to make some easy stuff with map reduce on this big value");
            var jsQuery = new RiakMapReduceQuery()
                .Inputs(Bucket)
                .MapJs(x => x.Name("Riak.mapValuesJson").Keep(true));
            try
            {
                var viaJsMapValuesJsonResult = RiakClient.MapReduce(jsQuery);
                var bigValueSpoiledByJs = viaJsMapValuesJsonResult.Value.PhaseResults.First().GetObjects().ToList()[0][0].Value;
            }
            catch (JsonReaderException e)
            {
                Console.WriteLine("Just as planned. JS convert max long to its float representation, so 9223372036854775807 becomes 9223372036854776000. We can use string instead of ints or use erlang instead of js.");
            }

            Console.WriteLine("Now try same func in Erlang. Integers are unbounded in Erlang");
            var erlangQuery = new RiakMapReduceQuery()
                .Inputs(Bucket)
                .MapErlang(x => x.Argument(true).ModFun("riak_kv_mapreduce", "map_object_value").Keep(true))
                .ReduceErlang(x => x.ModFun("riak_kv_mapreduce", "reduce_string_to_integer").Keep(true));

            var viaErlangMapValuesJsonResult = RiakClient.MapReduce(erlangQuery);
            var bigValueFromErlang = viaErlangMapValuesJsonResult.Value.PhaseResults.Last().GetObjects().ToList()[0][0].Value;
            Console.WriteLine("Is written long.MaxValue equals to obtained by Erlang map reduce value? {0}", _maxLong == bigValueFromErlang);
        }
    }
}