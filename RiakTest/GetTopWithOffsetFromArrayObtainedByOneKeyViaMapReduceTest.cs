using System;
using System.Collections.Generic;
using CorrugatedIron;
using CorrugatedIron.Models;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.MapReduce.Inputs;

namespace RiakTest
{
    public class GetTopWithOffsetFromArrayObtainedByOneKeyViaMapReduceTest : RiakTest
    {
        public const string Bucket = "get-top-with-offset-from-array-obtained-by-one-key-via-map-reduce-test";
        public const string Key = "my-key232";

        private readonly int _n;
        private readonly int _offset;
        private readonly int _limit;

        public GetTopWithOffsetFromArrayObtainedByOneKeyViaMapReduceTest(int n, int offset, int limit)
        {
            _n = n;
            _offset = offset;
            _limit = limit;
        }

        protected override void SetUp()
        {
            var sampleData = new List<GoodComment>();
            for (var i = 0; i < _n; i++)
            {
                sampleData.Add(new GoodComment{UserId = i, GoodId = i, Body = "comment body" + i, Answer = "answer " + i, DeletionReason = i, DepartmentId = i, VoteMinus = i, VotePlus = i});
            }
            var putResult = RiakClient.Put(new RiakObject(Bucket, Key, sampleData));
            if (!putResult.IsSuccess)
            {
                throw new SystemException();
            }
        }

        protected override void DoWork()
        {
            RiakResult<RiakMapReduceResult> result = null;
            Bench("Take top N elements with offset K from single object obtained by key using map reduce", bucket =>
                {
                    var rbki = new RiakBucketKeyInput();
                    rbki.AddBucketKey(Bucket, Key);
                    var query = new RiakMapReduceQuery()
                        .Inputs(rbki)
                        .MapJs(x => x
                            .Argument(string.Format(@"{{""offset"" : {0}, ""limit"" : {1}}}", _offset, _limit))
                            .Source(@"function(value, key, arg) { var parsedArg = JSON.parse(arg); return Riak.mapValuesJson(value)[0].slice(parsedArg.offset, parsedArg.offset + parsedArg.limit); }")
                            .Keep(true)
                        );
                    result = RiakClient.MapReduce(query);
                }, Bucket);
            if (!result.IsSuccess)
            {
                throw new SystemException();
            }
            PrintMapReduceResult(result);
        }

        protected override void TearDown()
        {
            RiakClient.DeleteBucket(Bucket);
        }

        private static void PrintMapReduceResult(RiakResult<RiakMapReduceResult> mapReduceResult)
        {
            foreach (var phase in mapReduceResult.Value.PhaseResults)
            {
                var phaseNumber = phase.Phase;
                var collection = phase.GetObjects();
                Console.WriteLine(phaseNumber);
                Console.WriteLine(string.Join(", ", collection));
            }
        }
    }
}