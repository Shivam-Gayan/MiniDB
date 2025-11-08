using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB.Engine.Storage
{
    public class TableManager
    {
        private readonly FileManager _fileManager;
        private readonly Dictionary<string, (Schema schema, List<int> pages)> _tables; // Table name to list of page IDs

        public TableManager(FileManager fileManager)
        {
            _fileManager = fileManager;
            _tables = [];
            LoadCatalog();
        }


        // Methods

        public void CreateTable(string tableName, Schema schema)
        {
            if (_tables.ContainsKey(tableName)) { 
                throw new InvalidOperationException($"Table '{tableName}' already exists.");
            }

            // Allocate initial page for the new table
            int firstPageId = _fileManager.AllocatePage(PageType.Data);
            _tables[tableName] = (schema, new List<int> { firstPageId });

            // Serialize schema and page list for catalog
            string schemaString = string.Join(",", schema.Columns.Zip(schema.ColumnTypes, (col,types) => $"{col}:{types}"));
            string pageList = string.Join(",", _tables[tableName].pages);

            // Create catalog record
            var catalogRecord = new Record(
                new Schema(
                    "Catalog",
                    ["TableName", "SchemaDef", "PageIds"],
                    [FieldType.String, FieldType.String, FieldType.String]
                ),
                [tableName, schemaString, pageList]
            );

            // Load meta page
            byte[] metaBytes = _fileManager.ReadPage(0);
            var metaPage = new Page();
            metaPage.Load(metaBytes);

            // Insert catalog record into meta page
            metaPage.TryInsertRecord(catalogRecord, out int slotIndex);
            _fileManager.WritePage(0, metaPage.GetBytes());
            _fileManager.Flush();

            Console.WriteLine($"Table '{tableName}' created successfully with first data page {firstPageId}.");

        }


        /*
         * TODO:
         * optimize meta page updation to find and update old record instead of inserting new one 
         */
        public RID Insert(string tableName, Record record)
        {
            if (!_tables.ContainsKey(tableName))
                throw new Exception($"Table '{tableName}' does not exist.");

            var (schema, pages) = _tables[tableName];

            if (!schema.Equals(record.Schema))
                throw new Exception("Record schema does not match table schema.");

            // Try inserting into existing pages
            foreach (int pageId in pages)
            {
                byte[] data = _fileManager.ReadPage(pageId);
                var page = new Page();
                page.Load(data);

                if (record.ToBytes().Length + 4 <= page.GetFreeSpace())
                {
                    page.TryInsertRecord(record, out int slot);
                    _fileManager.WritePage(pageId, page.GetBytes());
                    _fileManager.Flush();
                    return new RID(pageId, slot);
                }
            }

            // No space left -> allocate new page
            int newPageId = _fileManager.AllocatePage(PageType.Data);
            pages.Add(newPageId);
            _tables[tableName] = (schema, pages);

            // Update Meta page
            string schemaString = string.Join(",", schema.Columns.Zip(schema.ColumnTypes, (col, type) => $"{col}:{type}"));
            string pageList = string.Join(",", pages);

            var catalogSchema = new Schema(
                "Catalog",
                ["TableName", "SchemaDef", "PageIds"],
                [FieldType.String, FieldType.String, FieldType.String]
            );

            var catalogRecord = new Record(catalogSchema, tableName, schemaString, pageList);

            byte[] metaBytes = _fileManager.ReadPage(0);
            var metaPage = new Page();
            metaPage.Load(metaBytes);
            metaPage.TryInsertRecord(catalogRecord, out _);
            _fileManager.WritePage(0, metaPage.GetBytes());
            _fileManager.Flush();

            // Insert record into new page
            var newDataPage = new Page(newPageId, PageType.Data);
            newDataPage.TryInsertRecord(record, out int newSlot);
            _fileManager.WritePage(newPageId, newDataPage.GetBytes());
            _fileManager.Flush();

            return new RID(newPageId, newSlot);
        }


        public List<Record> SelectAll(string tableName)
        {
            if (!_tables.ContainsKey(tableName))
            {
                throw new Exception($"Table '{tableName}' does not exist.");
            }

            var (schema, pages) = _tables[tableName];
            var results = new List<Record>();

            foreach (int pageId in pages)
            {
                byte[] data = _fileManager.ReadPage(pageId);
                var page = new Page();
                page.Load(data);

                for (int slot = 0; slot < page.Header.SlotCount; slot++)
                {
                    try
                    {
                        var record = page.ReadRecord(schema, slot);
                        if (record != null)
                            results.Add(record);
                    }
                    catch
                    {
                        // In case of deleted/corrupted record, skip safely
                    }
                }
            }

            return results;
        }


        /*
         * after slot reuse implementation, update catalog saving logic
         */
        public void SaveCatalog()
        {
            // Save the catalog to a dedicated page in the file
        }

        private void LoadCatalog()
        {
            // Read meta page (page 0)
            byte[] metaBytes = _fileManager.ReadPage(0);
            var metaPage = new Page();
            metaPage.Load(metaBytes);

            // Define catalog schema
            var catalogSchema = new Schema(
                "Catalog",
                ["TableName", "SchemaDef", "PageIds"],
                [FieldType.String, FieldType.String, FieldType.String]
            );

            // Parse each record in the meta page
            for (int slot = 0; slot < metaPage.Header.SlotCount; slot++)
            {
                var record = metaPage.ReadRecord(catalogSchema, slot);

                string tableName = record.Values[0].ToString()!;
                string schemaString = record.Values[1].ToString()!;
                string pageListString = record.Values[2].ToString()!;

                // Parse schema string
                var columns = new List<string>();
                var types = new List<FieldType>();

                foreach (var part in schemaString.Split(',', StringSplitOptions.RemoveEmptyEntries))
                {
                    var kv = part.Split(':');
                    if (kv.Length != 2) continue;
                    columns.Add(kv[0]);
                    types.Add(ParseFieldType(kv[1]));
                }

                var schema = new Schema(tableName, columns, types);

                // Parse page list
                var pageIds = pageListString
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(int.Parse)
                    .ToList();

                // Register table in in-memory dictionary
                _tables[tableName] = (schema, pageIds);
            }
        }


        private FieldType ParseFieldType(string typeName)
        {
            return typeName.ToLower() switch
            {
                "int32" or "int" => FieldType.Integer,
                "string" => FieldType.String,
                "bool" or "boolean" => FieldType.Boolean,
                "double" => FieldType.Double,
                _ => throw new Exception($"Unknown field type: {typeName}")
            };
        }
    }
}
