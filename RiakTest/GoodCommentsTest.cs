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
    public class GoodCommentsTest : RiakTest
    {
        public const string Bucket = "secondary-indexes-test-bucket";

        private readonly int _commentsCount;
        private readonly int _departmentsCount;
        private readonly int _end;
        private readonly int _goodsCount;
        private readonly string _searchByIndexName;
        private readonly int _start;
        private readonly int _usersCount;

        public GoodCommentsTest(int commentsCount, int goodsCount, int usersCount, int departmentsCount, string searchByIndexName, int start, int end)
        {
            _commentsCount = commentsCount;
            _goodsCount = goodsCount;
            _usersCount = usersCount;
            _departmentsCount = departmentsCount;
            _searchByIndexName = searchByIndexName;
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
            var currentDate = DateTime.Now;
            var currentDateAsStringMin = DateAsString(currentDate.Subtract(new TimeSpan(7, 0, 0, 0)));
            var currentDateAsStringMax = DateAsString(currentDate);
            var a = RiakClient.IndexGet(Bucket, _searchByIndexName, "2" + currentDateAsStringMin, "2" + currentDateAsStringMax);
            RiakResult<RiakMapReduceResult> result = null;
            Bench("Get by secondary index", bucket =>
                {
                    var rbki = new RiakBinIndexRangeInput(Bucket, _searchByIndexName, "2" + currentDateAsStringMin, "2" + currentDateAsStringMax);
                    const string reduceSlice =
                        @"function(value, arg) { var parsedArg = JSON.parse(arg); var start = parsedArg.start; var end = parsedArg.end; if (end > value.length) { return value; } else { return value.slice(start, end); } }";
                    var reduceSliceArg = string.Format(@"{{""start"" : {0}, ""end"" : {1}}}", _start, _end);
                    var query = new RiakMapReduceQuery()
                        .Inputs(rbki)
                        .MapJs(x => x.Name("Riak.mapValuesJson").Keep(true))
                        .ReduceJs(x => x.Argument(reduceSliceArg).Source(
                            @"function(val, arg) { 
                var parsedArg = JSON.parse(arg);
                var start = parsedArg.start;
                var end = parsedArg.end;
                return Riak.reduceSlice(Riak.reduceSort(val, function(a,b) {
                    return a.Id - b.Id
                }), [start, end]);
            }").Keep(true))
                        /*.ReduceJs(x => x.Argument("function(a,b) { return a.Id - b.Id}").Name("Riak.reduceSort").Keep(true))*/
                        /*.ReduceJs(x => x.Argument(reduceSliceArg).Source(reduceSlice).Keep(true))*/
                        /*.ReduceJs(x => x.Argument(reduceSliceArg).Source(@"function(value, arg) { var parsedArg = JSON.parse(arg); var start = parsedArg.start; var end = parsedArg.end; return value.slice(start,end); }").Keep(true))*/
                        /*.MapJs(x => x.Source("function(value, key, arg) { return [value]; }").Keep(true))*/
                        /*.MapJs(x => x
                                        .Source(@"function(value, key, arg) { return [[ Riak.mapValuesJson(value)[0] ]]; }")
                                        .Keep(true)
                                    )*/
                        /*.ReduceJs(x => x
                                    .Argument(string.Format(@"{{""offset"" : {0}, ""limit"" : {1}}}", 5, 10))
                                        .Source(@"function(value, key, arg) { var parsedArg = JSON.parse(arg); return [ Riak.mapValuesJson(value)[0].slice(parsedArg.offset, parsedArg.offset + parsedArg.limit) ]; }")
                                        .Keep(true)
                                    )*/;
                    result = RiakClient.MapReduce(query);
                }, Bucket);
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
                var collection = phase.GetObjects();
                Console.WriteLine(phaseNumber);
                Console.WriteLine(string.Join(", ", collection));
            }
        }

        protected override void TearDown()
        {
            var result = RiakClient.IndexGet(Bucket, "$bucket", "");
            if (!result.IsSuccess)
            {
                throw new SystemException();
            }
            foreach (var value in result.Value)
            {
                RiakClient.Delete(Bucket, value);
            }
        }
    }
}