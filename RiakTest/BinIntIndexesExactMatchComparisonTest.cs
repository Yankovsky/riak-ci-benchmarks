using System.Globalization;
using System.Linq;
using CorrugatedIron.Models;
using CorrugatedIron.Util;

namespace RiakTest
{
    public class BinIntIndexesExactMatchComparisonTest : RiakTestBase
    {
        private const string BinaryIndex = "binary_index";
        private const string IntegerIndex = "integer_index";
        private readonly int _differentSecondaryIndexesCount;
        private readonly int _n;

        public BinIntIndexesExactMatchComparisonTest(int n, int differentSecondaryIndexesCount, bool setUp = true, bool tearDown = true)
            : base("int-bin-indexes-comparison-bucket", setUp, tearDown)
        {
            _n = n;
            _differentSecondaryIndexesCount = differentSecondaryIndexesCount;
        }

        protected override void SetUp()
        {
            var riakObjects = DataGenerator.GenerateGoodsComments(_n).Select(x => new RiakObject(Bucket, x.Id.ToString(CultureInfo.InvariantCulture), x)).ToList();
            for (var i = 0; i < _n; i++)
            {
                riakObjects[i].IntIndex(IntegerIndex).Set(i%_differentSecondaryIndexesCount);
                riakObjects[i].BinIndex(BinaryIndex).Set((i%_differentSecondaryIndexesCount).ToString(CultureInfo.InvariantCulture) + "binaryIndex");
            }
            RiakClient.Put(riakObjects);
        }

        protected override void DoWork()
        {
            var someIntegerIndex = _differentSecondaryIndexesCount/2;
            var correspondingBinaryIndex = someIntegerIndex.ToString(CultureInfo.InvariantCulture) + "binaryIndex";
            Bench("Get by integer index", () => RiakClient.IndexGet(Bucket, IntegerIndex, someIntegerIndex));
            Bench("Get by binary index", () => RiakClient.IndexGet(Bucket, BinaryIndex, correspondingBinaryIndex));
        }
    }
}