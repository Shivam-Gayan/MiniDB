using DB.Engine.Storage;

namespace DB.Engine.Index
{
    public sealed class IndexFileHandle
    {
        public FileManager FileManager { get; }
        public int FirstMetaPageId { get; set; }

        public IndexFileHandle(FileManager fileManager, int firstMetaPageId)
        {
            FileManager = fileManager ?? throw new ArgumentNullException(nameof(fileManager));
            FirstMetaPageId = firstMetaPageId;
        }
    }
}
