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
            //new WriteToMultipleBucketsTest(1000, 143, 47, 16).Run(true, false);
            //new SecondaryIndexesTest(100000, 10000, 2500, 6).Run(setUp:false,tearDown:false);
            
            new GoodCommentsTest(1000, 200, 20, 2, "UserId", 0, 40).Run(false, false);
            
            Console.WriteLine("Press enter to continue...");
            Console.Read();
        }
    }
}