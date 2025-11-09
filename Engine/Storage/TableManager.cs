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

            UpdateCatalogEntry(tableName, schema, [firstPageId]);

            Console.WriteLine($"Table '{tableName}' created successfully with first data page {firstPageId}.");

        }

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
            UpdateCatalogEntry(tableName, schema, pages);

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

        public void SaveCatalog()
        {
            // Step 1: Load Meta page
            byte[] metaBytes = _fileManager.ReadPage(0);
            var metaPage = new Page();
            metaPage.Load(metaBytes);

            // Step 2: Define schema
            var catalogSchema = new Schema(
                "Catalog",
                ["TableName", "SchemaDef", "PageIds"],
                [FieldType.String, FieldType.String, FieldType.String]
            );

            // Step 3: Mark existing entries as deleted
            for (int slot = 0; slot < metaPage.Header.SlotCount; slot++)
            {
                metaPage.DeleteRecord(slot);
            }

            // Step 4: Rewrite all current tables
            foreach (var kvp in _tables)
            {
                string tableName = kvp.Key;
                var (schema, pages) = kvp.Value;

                UpdateCatalogEntry(tableName, schema, pages);
            }

            // Step 5: Flush to disk
            _fileManager.Flush();
            Console.WriteLine("Catalog saved successfully.");
        }


        private void UpdateCatalogEntry(string tableName, Schema schema, List<int> pages)
        {
            byte[] metaBytes = _fileManager.ReadPage(0);
            var metaPage = new Page();
            metaPage.Load(metaBytes);

            var catalogSchema = new Schema(
                "Catalog",
                ["TableName", "SchemaDef", "PageIds"],
                [FieldType.String, FieldType.String, FieldType.String]
            );

            string schemaString = string.Join(",", schema.Columns.Zip(schema.ColumnTypes.Zip(schema.IsNullable),(col, t) => $"{col}:{t.First}:{(t.Second ? "NULL" : "NOTNULL")}"));
            string pageList = string.Join(",", pages);
            var newRecord = new Record(catalogSchema, [tableName, schemaString, pageList]);

            // find existing record slot

            for (int slot =0; slot < metaPage.Header.SlotCount; slot++)
            {
                try
                {
                    var existingRecord = metaPage.ReadRecord(catalogSchema, slot);
                    if (existingRecord == null) continue;

                    string existingTableName = existingRecord.Values[0].ToString()!;
                    if (existingTableName == tableName)
                    {
                        metaPage.DeleteRecord(slot);
                        break;
                    }
                }
                catch
                {
                    // skip corrupted/deleted records
                }

            }

            metaPage.TryInsertRecord(newRecord, out _);
            _fileManager.WritePage(0, metaPage.GetBytes());
            _fileManager.Flush();

        }


        private void LoadCatalog()
        {
            // Read Meta (Catalog) page
            byte[] metaBytes = _fileManager.ReadPage(0);
            var metaPage = new Page();
            metaPage.Load(metaBytes);

            // Define the catalog schema (system-level)
            var catalogSchema = new Schema(
                "Catalog",
                ["TableName", "SchemaDef", "PageIds"],
                [FieldType.String, FieldType.String, FieldType.String]
            );

            // Parse each record stored in the Meta page
            for (int slot = 0; slot < metaPage.Header.SlotCount; slot++)
            {
                var record = metaPage.ReadRecord(catalogSchema, slot);
                if (record == null) continue; // skip deleted or invalid records

                string tableName = record.Values[0].ToString()!;
                string schemaString = record.Values[1].ToString()!;
                string pageListString = record.Values[2].ToString()!;

                // Parse schema string into columns, types, and nullability
                var columns = new List<string>();
                var types = new List<FieldType>();
                var isNullable = new List<bool>();

                foreach (var part in schemaString.Split(',', StringSplitOptions.RemoveEmptyEntries))
                {
                    var kv = part.Split(':');
                    if (kv.Length < 2) continue; // skip malformed entries

                    columns.Add(kv[0]);
                    types.Add(ParseFieldType(kv[1]));

                    // If nullability info exists 
                    if (kv.Length >= 3)
                        isNullable.Add(kv[2].Equals("NULL", StringComparison.OrdinalIgnoreCase));
                    else
                        isNullable.Add(false); // default: NOT NULL
                }

                // Build schema object from parsed info
                var schema = new Schema(tableName, columns, types, isNullable);

                // Parse page list into integers
                var pageIds = pageListString
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(int.Parse)
                    .ToList();

                // Register table in memory
                _tables[tableName] = (schema, pageIds);
            }
        }

        public void VacuumTable(string tableName)
        {
            if (!_tables.ContainsKey(tableName))
                throw new Exception($"Table '{tableName}' not found.");

            var (_, pages) = _tables[tableName];
            foreach (var pageId in pages)
            {
                byte[] data = _fileManager.ReadPage(pageId);
                var page = new Page();
                page.Load(data);

                page.Vacuum();

                _fileManager.WritePage(pageId, page.GetBytes());
            }

            _fileManager.Flush();
            Console.WriteLine($"Table '{tableName}' vacuumed successfully.");
        }

        public void VacuumAll()
        {
            foreach (var tableName in _tables.Keys)
                VacuumTable(tableName);

            Console.WriteLine("All tables vacuumed successfully.");
        }

        private static FieldType ParseFieldType(string typeName)
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
