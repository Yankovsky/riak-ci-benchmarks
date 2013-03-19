using System;
using System.Globalization;
using System.Linq;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class BinIntIndexesRangeQueryComparisonTest : RiakTestBase
    {
        private const string BinaryIndex = "binary_index";
        private const string IntegerIndex = "integer_index";
        private readonly int _differentSecondaryIndexesCount;
        private readonly int _n;
        private readonly int _rangeMax;
        private readonly int _rangeMin;
        private readonly int _expectedKeysCount;

        public BinIntIndexesRangeQueryComparisonTest(int n, int differentSecondaryIndexesCount, int rangeMin, int rangeMax, int expectedKeysCount, bool setUp = true, bool tearDown = true)
            : base("int-bin-indexes-range-comparison-bucket", setUp, tearDown)
        {
            _n = n;
            _differentSecondaryIndexesCount = differentSecondaryIndexesCount;
            _rangeMin = rangeMin;
            _rangeMax = rangeMax;
            _expectedKeysCount = expectedKeysCount;
        }

        protected override void SetUp()
        {
            var riakObjects = DataGenerator.GenerateGoodsComments(_n).Select(x => new RiakObject(Bucket, x.Id.ToString(CultureInfo.InvariantCulture), x)).ToList();
            for (var i = 0; i < _n; i++)
            {
                riakObjects[i].IntIndex(IntegerIndex).Set(i%_differentSecondaryIndexesCount);
                riakObjects[i].BinIndex(BinaryIndex).Set(string.Format("binaryIndex{0:000000000}", i % _differentSecondaryIndexesCount));
            }
            RiakClient.Put(riakObjects);
        }

        protected override void DoWork()
        {
            Bench("Get by integer index", () =>
                {
                    var result = RiakClient.IndexGet(Bucket, IntegerIndex, _rangeMin, _rangeMax);
                    if (result.Value.Count != _expectedKeysCount)
                    {
                        throw new SystemException();
                    }
                });
            var rangeMinBin = string.Format("binaryIndex{0:000000000}", _rangeMin);
            var rangeMaxBin = string.Format("binaryIndex{0:000000000}", _rangeMax);
            Bench("Get by binary index", () =>
            {
                var result = RiakClient.IndexGet(Bucket, BinaryIndex, rangeMinBin, rangeMaxBin);
                if (result.Value.Count != _expectedKeysCount)
                {
                    throw new SystemException();
                }
            });
        }
    }
}