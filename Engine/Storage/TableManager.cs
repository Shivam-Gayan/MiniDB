using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public class TableManager
    {
        FileManager FileManager;
        Dictionary<string, List<int>> Catalog; // Table name to list of page IDs

        // Methods

        void CreateTable(string tableName)
        {
            // Create a new table and initialize its catalog entry
        }

        int Insert(string tableName, Record record)
        {
            // Insert a record into the specified table and return the record ID
            return 0;
        }

        List<Record> SelectAll(string tableName)
        {
            // Select and return all records from the specified table
            return new List<Record>();
        }

        void SaveCatalog()
        {
            // Save the catalog to a dedicated page in the file
        }

        void LoadCatalog()
        {
            // Load the catalog from the dedicated page in the file
        }


    }
}
