using System;
using System.Collections.Generic;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class GetArrayObtainedByOneKeyTest : RiakTestBase
    {
        public const string Key = "my-key";

        private readonly int _n;

        public GetArrayObtainedByOneKeyTest(int n, bool setUp = true, bool tearDown = true)
            : base("get-array-obtained-by-one-key-test", setUp, tearDown)
        {
            _n = n;
        }   

        protected override void SetUp()
        {
            var sampleData = new List<GoodComment>();
            for (var i = 0; i < _n; i++)
            {
                sampleData.Add(new GoodComment
                    {
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
            var putResult = RiakClient.Put(new RiakObject(Bucket, Key, sampleData));
            if (!putResult.IsSuccess)
            {
                throw new SystemException();
            }
        }

        protected override void DoWork()
        {
            Bench("Take single object containing array by key", () =>
                {
                    var result = RiakClient.Get(Bucket, Key);
                    if (!result.IsSuccess)
                    {
                        throw new SystemException();
                    }
                });
        }
    }
}