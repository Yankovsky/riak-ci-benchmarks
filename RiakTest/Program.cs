using System;

namespace RiakTest
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //new WriteThroughputTest(1000).Run();
            //new GetTopWithOffsetFromArrayObtainedByOneKeyViaMapReduceTest(10, 2, 3).Run();
            //new GetArrayObtainedByOneKeyTest(3000).Run();
            //new GetGoodsByUserIdUsingRiakSearch(100, 20, 30).Run(true, true);
            //new SimpleRiakSearchTest(1000, 100).Run(false, true);
            //new WriteToMultipleBucketsBenchmark(1000, 143, 47, 16).Run(true, false);
            //new SecondaryIndexesTest(100000, 10000, 2500, 6).Run(setUp:false,tearDown:false);
            
            // Change params here to customize your test
            // at first run with params (true,false) to setup sample data, then change back to false, false
            //new GoodsCommentsAllTogetherTest(100000, 2000, 200, 20, "DepartmentId", 15, 97, 99, false, false).Run();
            
            //new DeleteBucket("delete-bucket", 1000).Run();

            Console.WriteLine("Press enter to continue...");
            Console.Read();
        }
    }
}