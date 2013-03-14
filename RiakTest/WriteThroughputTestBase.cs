using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class WriteThroughputTestBase : RiakTestBase
    {
        private readonly int _n;
        private IEnumerable<RiakObject> _riakObjects;

        public WriteThroughputTestBase(int n) : base("write-test-bucket")
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
            Bench("Write batch", () => { riakResults = RiakClient.Put(_riakObjects); });
            if (riakResults.Any(riakResult => !riakResult.IsSuccess))
            {
                throw new SystemException();
            }
        }
    }
}