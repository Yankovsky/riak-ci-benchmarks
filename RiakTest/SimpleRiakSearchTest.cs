using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron;
using CorrugatedIron.Models;
using CorrugatedIron.Models.Search;

namespace RiakTest
{
    public class SimpleRiakSearchTest : RiakTestBase
    {
        private readonly int _commentsCount;
        private readonly int _usersCount;

        public SimpleRiakSearchTest(int commentsCount, int usersCount) : base("search_bucket")
        {
            _commentsCount = commentsCount;
            _usersCount = usersCount;
        }

        protected override void SetUp()
        {
            var riakObjects = new List<RiakObject>();
            for (var i = 0; i < _commentsCount; i++)
            {
                riakObjects.Add(new RiakObject(Bucket, i.ToString(CultureInfo.InvariantCulture), new GoodComment
                    {
                        GoodId = i,
                        UserId = i % _usersCount,
                        Body = "comment body",
                        Answer = "answer " + i,
                        DeletionReason = i,
                        DepartmentId = i,
                        VoteMinus = i,
                        VotePlus = i
                    }));
            }
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
                    var search = new RiakFluentSearch(Bucket, "UserId").Search("23").Build();
                    result = RiakClient.Search(new RiakSearchRequest {Query = search, Rows = 10});
                    if (!result.IsSuccess)
                    {
                        throw new SystemException();
                    }
                });
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