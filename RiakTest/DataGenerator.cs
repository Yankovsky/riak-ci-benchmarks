using System;
using System.Collections.Generic;
using System.Reflection;

namespace RiakTest
{
    public static class DataGenerator
    {
        public static List<GoodComment> GenerateGoodsComments(int n, int goodsCount, int usersCount, int departmentsCount)
        {
            var result = new List<GoodComment>(n);
            for (var i = 0; i < n; i++)
            {
                var goodId = i % goodsCount;
                var userId = i % usersCount;
                var departmentId = i % departmentsCount;
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
                    CreatedAt = new DateTime(2013, 3, i % 29 + 1)
                };
                result.Add(goodComment);
            }
            return result;
        }

        public static List<GoodComment> GenerateGoodsComments(int n)
        {
            return GenerateGoodsComments(n, n, n, n);
        }
    }
}