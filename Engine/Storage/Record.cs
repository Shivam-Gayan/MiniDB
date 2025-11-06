using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public class Record
    {
        byte[] Payload { get; set; }

        static Record FromFields(string[] fields)
        {
            // Serialize fields into a byte array payload
            return new Record();
        }

        string ToDisplayString()
        {
            // Deserialize payload back into fields and format as string
            return string.Empty;
        }
    }
}
