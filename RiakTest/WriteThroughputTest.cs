using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class WriteThroughputTest : RiakTest
    {
        public const string Bucket = "write-test-bucket";

        private readonly int _n;
        private IEnumerable<RiakObject> _riakObjects;

        public WriteThroughputTest(int n)
        {
            _n = n;
        }

        protected override void SetUp()
        {
        }

        protected override void DoWork()
        {
            var sampleData = new List<GoodComment>();
            for (var i = 0; i < _n; i++)
            {
                sampleData.Add(new GoodComment
                    {
                        Id = i,
                        UserId = i,
                        GoodId = i,
                        Body = "comment body" + i,
                        Answer = "answer " + i,
                        DeletionReason = i,
                        DepartmentId = i,
                        VoteMinus = i,
                        VotePlus = i
                    });
            }
            _riakObjects = sampleData.Select(x => new RiakObject(Bucket, x.Id.ToString(CultureInfo.InvariantCulture), x));
            IEnumerable<RiakResult<RiakObject>> riakResults = null;
            Bench("Write batch", bucket => { riakResults = RiakClient.Put(_riakObjects); }, Bucket);
            ;
            if (riakResults.Any(riakResult => !riakResult.IsSuccess))
            {
                throw new SystemException();
            }
        }

        protected override void TearDown()
        {
            for (int i = 0; i < _n; i++)
            {
                RiakClient.Delete(Bucket, i.ToString(CultureInfo.InvariantCulture));
            }
        }
    }
}