using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class SecondaryIndexesTest : RiakTestBase
    {
        private readonly int _commentsCount;
        private readonly int _departmentsCount;
        private readonly int _goodsCount;
        private readonly int _usersCount;

        public SecondaryIndexesTest(int commentsCount, int goodsCount, int usersCount, int departmentsCount) : base("secondary-indexes-test-bucket")
        {
            _commentsCount = commentsCount;
            _goodsCount = goodsCount;
            _usersCount = usersCount;
            _departmentsCount = departmentsCount;
        }

        protected override void SetUp()
        {
            var sampleData = new List<GoodComment>();
            for (var i = 0; i < _commentsCount; i++)
            {
                var goodId = i%_goodsCount;
                var userId = i%_usersCount;
                var departmentId = i%_departmentsCount;
                var goodComment = new GoodComment
                    {
                        Id = i,
                        GoodId = goodId,
                        UserId = userId,
                        Body = string.Format("comment by user {0} on good {1}", userId, goodId),
                        Answer = "answer " + i,
                        DeletionReason = i,
                        DepartmentId = departmentId,
                        VoteMinus = i,
                        VotePlus = i
                    };
                sampleData.Add(goodComment);
            }
            var riakObjects = sampleData.Select(x =>
                {
                    var indexedRiakObject = new RiakObject(Bucket, x.Id.ToString(CultureInfo.InvariantCulture), x);
                    indexedRiakObject.IntIndex("good-id").Set(x.GoodId);
                    indexedRiakObject.IntIndex("user-id").Set(x.UserId);
                    indexedRiakObject.IntIndex("department-id").Set(x.DepartmentId);
                    return indexedRiakObject;
                });
            var putResults = RiakClient.Put(riakObjects);
            if (putResults.Any(putResult => !putResult.IsSuccess))
            {
                throw new SystemException();
            }
        }

        protected override void DoWork()
        {
            RiakResult<IList<string>> result = null;
            Bench("Get by secondary index", () => { result = RiakClient.IndexGet(Bucket, "user-id", 2); });
            if (!result.IsSuccess)
            {
                throw new SystemException();
            }
        }
    }
}