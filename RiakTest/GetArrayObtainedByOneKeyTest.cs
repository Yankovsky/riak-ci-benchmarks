using System;
using System.Collections.Generic;
using CorrugatedIron.Models;

namespace RiakTest
{
    public class GetArrayObtainedByOneKeyTest : RiakTest
    {
        public const string Bucket = "get-array-obtained-by-one-key-test";
        public const string Key = "my-key";

        private readonly int _n;

        public GetArrayObtainedByOneKeyTest(int n)
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
            Bench("Take single object containing array by key", bucket =>
                {
                    var result = RiakClient.Get(Bucket, Key);
                    if (!result.IsSuccess)
                    {
                        throw new SystemException();
                    }
                }, Bucket);
        }

        protected override void TearDown()
        {
            RiakClient.DeleteBucket(Bucket);
        }
    }
}