using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Search;

namespace RiakTest
{
    public class GetGoodsByUserIdUsingRiakSearch : RiakTestBase
    {

        private readonly int _commentsCount;
        private readonly int _goodsCount;
        private readonly int _usersCount;

        public GetGoodsByUserIdUsingRiakSearch(int commentsCount, int goodsCount, int usersCount)
            : base("goods_comments")
        {
            _commentsCount = commentsCount;
            _goodsCount = goodsCount;
            _usersCount = usersCount;
        }

        protected override void SetUp()
        {
            var sampleData = new List<GoodComment>();
            for (var i = 0; i < _commentsCount; i++)
            {
                var goodId = i%_goodsCount;
                var userId = i%_usersCount;
                var goodComment = new GoodComment
                    {
                        GoodId = goodId,
                        UserId = userId,
                        Body = string.Format("comment by user {0} on good {1}", userId, goodId),
                        Answer = "answer " + i,
                        DeletionReason = i,
                        DepartmentId = i,
                        VoteMinus = i,
                        VotePlus = i
                    };
                sampleData.Add(goodComment);
            }
            var riakObjects = sampleData.GroupBy(x => x.GoodId).Select(x => new RiakObject(Bucket, x.Key.ToString(CultureInfo.InvariantCulture), x.ToList()));
            var putResults = RiakClient.Put(riakObjects);
            if (putResults.Any(putResult => !putResult.IsSuccess))
            {
                throw new SystemException();
            }
        }

        protected override void DoWork()
        {
            RiakResult<RiakSearchResult> result = null;
            Bench("Take ", () =>
                {
                    var search = new RiakFluentSearch(Bucket, "UserId").Search("2").Build();
                    result = RiakClient.Search(new RiakSearchRequest {Query = search});
                    if (!result.IsSuccess)
                    {
                        throw new SystemException();
                    }
                });
            Console.WriteLine(result.Value.NumFound);
            PrintSearchResults(result);
        }

        private static void PrintSearchResults(RiakResult<RiakSearchResult> result)
        {
            foreach (var document in result.Value.Documents)
            {
                Console.WriteLine("Entry with id = {0}", document.Id.Value);
                foreach (var field in document.Fields)
                {
                    Console.WriteLine("{0} - {1}", field.Key, field.Value);
                }
            }
        }
    }
}