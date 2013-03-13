using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class WriteToMultipleBucketsTest : RiakTest
    {
        public const string CommentsByGoodIdBucket = "comments-by-good-id-bucket";
        public const string CommentsByUserIdBucket = "comments-by-user-id-bucket";
        public const string CommentsByDepartmentIdBucket = "comments-by-department-id-bucket";

        private readonly int _commentsCount;
        private readonly int _departmentsCount;
        private readonly int _goodsCount;
        private readonly int _usersCount;
        private List<GoodComment> _sampleData;

        public WriteToMultipleBucketsTest(int commentsCount, int goodsCount, int usersCount, int departmentsCount)
        {
            _commentsCount = commentsCount;
            _goodsCount = goodsCount;
            _usersCount = usersCount;
            _departmentsCount = departmentsCount;
        }

        protected override void SetUp()
        {
            _sampleData = new List<GoodComment>();
            for (var i = 0; i < _commentsCount; i++)
            {
                var goodId = i % _goodsCount;
                var userId = i % _usersCount;
                var departmentId = i % _departmentsCount;
                var goodComment = new GoodComment
                {
                    GoodId = goodId,
                    UserId = userId,
                    Body = string.Format("comment by user {0} on good {1}", userId, goodId),
                    Answer = "answer " + i,
                    DeletionReason = i,
                    DepartmentId = departmentId,
                    VoteMinus = i,
                    VotePlus = i
                };
                _sampleData.Add(goodComment);
            }
        }

        protected override void DoWork()
        {
            var bucketGroupingInfos = new List<Tuple<string, Func<GoodComment, int>>>
                {
                    new Tuple<string, Func<GoodComment, int>>(CommentsByGoodIdBucket, x => x.GoodId),
                    new Tuple<string, Func<GoodComment, int>>(CommentsByUserIdBucket, x => x.UserId),
                    new Tuple<string, Func<GoodComment, int>>(CommentsByDepartmentIdBucket, x => x.DepartmentId)
                };
            foreach (var bucketGroupingInfo in bucketGroupingInfos)
            {
                Bench("Take ", b =>
                    {
                        var riakObjects = _sampleData.GroupBy(bucketGroupingInfo.Item2).Select(x =>
                            {
                                var indexedRiakObject = new RiakObject(b, x.Key.ToString(CultureInfo.InvariantCulture), x.ToList());
                                return indexedRiakObject;
                            });
                        var putResults = RiakClient.Put(riakObjects);
                        if (putResults.Any(putResult => !putResult.IsSuccess))
                        {
                            throw new SystemException();
                        }
                    }, bucketGroupingInfo.Item1);
            }
        }

        protected override void TearDown()
        {
            var buckets = new List<string> {CommentsByGoodIdBucket, CommentsByUserIdBucket, CommentsByDepartmentIdBucket};
            foreach (var bucket in buckets)
            {
                RiakClient.DeleteBucket(bucket);
            }
        }
    }
}