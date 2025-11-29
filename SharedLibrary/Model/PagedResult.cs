namespace SharedLibrary.Model
{
    public class PagedResult<T>
    {
        public IReadOnlyList<T> Items { get; set; }
        public int TotalCount { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public int TotalPages => (int)Math.Ceiling(TotalCount / (double)PageSize);
        public bool HasPreviousPage => PageNumber > 1;
        public bool HasNextPage => PageNumber < TotalPages;

        public PagedResult(IReadOnlyList<T> items, int totalCount, int pageNumber, int pageSize)
        {
            Items = items ?? new List<T>().AsReadOnly();
            TotalCount = totalCount;
            PageNumber = pageNumber;
            PageSize = pageSize;
        }

        public PagedResult() : this(new List<T>().AsReadOnly(), 0, 1, 20)
        {
        }

        public static PagedResult<T> Empty(int pageSize = 20)
        {
            return new PagedResult<T>(new List<T>().AsReadOnly(), 0, 1, pageSize);
        }

        public static PagedResult<T> Create(IEnumerable<T> items, int totalCount, int pageNumber, int pageSize)
        {
            var itemList = items?.ToList().AsReadOnly() ?? new List<T>().AsReadOnly();
            return new PagedResult<T>(itemList, totalCount, pageNumber, pageSize);
        }
    }
}
