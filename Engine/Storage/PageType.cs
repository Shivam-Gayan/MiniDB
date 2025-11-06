using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public enum PageType
    {
        Meta, // For metadata/catalog pages
        Data, // For table data 
        Index, // For index nodes 
        Free, // unused/free pages
    }
}
