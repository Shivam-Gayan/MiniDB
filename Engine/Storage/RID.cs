using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public class RID
    {
        int PageID { get; set; }
        short SlotID { get; set; }
    }
}
