using System;

namespace RiakTest
{
    public class GoodComment
    {
        public int Id { get; set; }
        public int GoodId { get; set; }
        public int UserId { get; set; }
        public string Body { get; set; }
        public int DeletionReason { get; set; }
        public int DepartmentId { get; set; }
        public string Answer { get; set; }
        public int VotePlus { get; set; }
        public int VoteMinus { get; set; }
        public DateTime CreatedAt { get; set; }
    }
}