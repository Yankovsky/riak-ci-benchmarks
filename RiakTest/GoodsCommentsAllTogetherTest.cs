using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CorrugatedIron;
using CorrugatedIron.Models;
using CorrugatedIron.Models.MapReduce;
using CorrugatedIron.Models.MapReduce.Inputs;

namespace RiakTest
{
    public class GoodsCommentsAllTogetherTest : RiakTestBase
    {
        private readonly int _commentsCount;
        private readonly int _departmentsCount;
        private readonly int _end;
        private readonly int _goodsCount;
        private readonly int _searchByIndexId;
        private readonly string _searchByIndexName;
        private readonly int _start;
        private readonly int _usersCount;

        public GoodsCommentsAllTogetherTest(int commentsCount, int goodsCount, int usersCount, int departmentsCount, string searchByIndexName, int searchByIndexId, int start, int end,
                                                bool setUp = true, bool tearDown = true) : base("secondary-indexes-test-bucket", setUp, tearDown)
        {
            _commentsCount = commentsCount;
            _goodsCount = goodsCount;
            _usersCount = usersCount;
            _departmentsCount = departmentsCount;
            _searchByIndexName = searchByIndexName;
            _searchByIndexId = searchByIndexId;
            _start = start;
            _end = end;
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
                        VotePlus = i,
                        CreatedAt = new DateTime(2013, 3, i%29 + 1)
                    };
                sampleData.Add(goodComment);
            }
            var riakObjects = sampleData.Select(x =>
                {
                    var indexedRiakObject = new RiakObject(Bucket, x.Id.ToString(CultureInfo.InvariantCulture), x);
                    var dateAsString = DateAsString(x.CreatedAt);
                    // I want to use integer for such purposes, but currently CI doesn't support integers > int.maxValue
                    // see https://github.com/DistributedNonsense/CorrugatedIron/issues/107
                    // Secondary indexes in format {index}{datetime in YYYYMMDD format}
                    indexedRiakObject.BinIndex("GoodId").Set(x.GoodId + dateAsString);
                    indexedRiakObject.BinIndex("UserId").Set(x.UserId + dateAsString);
                    indexedRiakObject.BinIndex("DepartmentId").Set(x.DepartmentId + dateAsString);
                    return indexedRiakObject;
                });
            var putResults = RiakClient.Put(riakObjects);
            if (putResults.Any(putResult => !putResult.IsSuccess))
            {
                throw new SystemException();
            }
        }

        private static string DateAsString(DateTime date)
        {
            return string.Format("{0:0000}{1:00}{2:00}", date.Year, date.Month, date.Day);
        }

        protected override void DoWork()
        {
            var currentDate = new DateTime(2013, 3, 15);
            var rangeMin = _searchByIndexId + DateAsString(currentDate.Subtract(new TimeSpan(7, 0, 0, 0)));
            var rangeMax = _searchByIndexId + DateAsString(currentDate);

            // e.g. {"start":5,"end":10}
            var offsetLimitArg = string.Format(@"{{""start"" : {0}, ""end"" : {1}}}", _start, _end);

            // I wanted to pass [start,end] args to Riak.reduceSlice, but can't figure how on current moment. So I created my own wrapper for args
            // see https://github.com/DistributedNonsense/CorrugatedIron/issues/106

            // I wanted to create two separate reduce phases - first for ordering and second for offset/limit, but I getting strange results.
            // These phases should be executed on single machine with all data obtained.
            // There is some additional param reduce_phase_only_1, but it doesn't work with CI, or i don't know how to use it with CI.
            // So I merge two reduce phases into one. Now I have correct results.
            const string orderByCreateAtFunction = @"
function(val, arg) { 
    return Riak.reduceSort(val, function(a,b) {
        var aStr = a.CreatedAt;
        var bStr = b.CreatedAt;
        if (aStr > bStr) {
            return 1;
        } else if (aStr == bStr) {
            return 0;
        }
        return -1;
    });
}";
            const string sliceFunction = @"
function(val, arg) { 
    var parsedArg = JSON.parse(arg);
    var start = parsedArg.start;
    var end = parsedArg.end;
    return Riak.reduceSlice(val, [start, end]);
}
";

            RiakResult<RiakMapReduceResult> result = null;
            Bench(string.Format("Get goods comments by {0} {1}, sort by CreatedAt date and then take elements from {2} to {3}", _searchByIndexName, _searchByIndexId, _start, _end), () =>
                {
                    var siDateRangeInput = new RiakBinIndexRangeInput(Bucket, _searchByIndexName, rangeMin, rangeMax);

                    var query = new RiakMapReduceQuery()
                        .Inputs(siDateRangeInput)
                        .MapJs(x => x.Name("Riak.mapValuesJson"))
                        .ReduceJs(x => x.Source(orderByCreateAtFunction).Keep(true))
                        .ReduceJs(x => x.Argument(offsetLimitArg).Source(sliceFunction).Keep(true));
                    result = RiakClient.MapReduce(query);
                });
            if (!result.IsSuccess)
            {
                throw new SystemException();
            }
            PrintMapReduceResult(result);
        }

        private static void PrintMapReduceResult(RiakResult<RiakMapReduceResult> mapReduceResult)
        {
            foreach (var phase in mapReduceResult.Value.PhaseResults)
            {
                var phaseNumber = phase.Phase;
                var collection = phase.GetObjects().ToList();
                Console.WriteLine(phaseNumber);
                Console.WriteLine(string.Join(", ", collection));
            }
        }
    }
}