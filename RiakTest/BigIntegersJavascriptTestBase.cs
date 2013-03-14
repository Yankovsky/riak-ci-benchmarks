using CorrugatedIron.Models;

namespace RiakTest
{
    /// <summary>
    /// There are problems with big integers in javascript. We can loose values and cannot rely on integer indexes in javascript map reduce phases.
    /// http://blog.8thlight.com/myles-megyesi/2013/01/18/riak-mapreduce.html
    /// http://ecma262-5.com/ELS5_HTML.htm#Section_8.5
    /// </summary>
    public class BigIntegersJavascriptTestBase : RiakTestBase
    {
        private long _maxLong = long.MaxValue;

        public BigIntegersJavascriptTestBase(string bucket) : base(bucket)
        {
        }

        protected override void SetUp()
        {

        }

        protected override void DoWork()
        {
            throw new System.NotImplementedException();
        }
    }
}