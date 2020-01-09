using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Linq;
using System.Web;

namespace SuperSCADA.Models
{
    public static class kolap
    {
        public static string conn = System.Configuration.ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
        static Dictionary<string, Dictionary<string, table_info>> tables = new Dictionary<string, Dictionary<string, table_info>>();
        //public static Dictionary<string, Dictionary<string, table_info>> tables = new Dictionary<string, Dictionary<string, table_info>>();
        //public static Dictionary<string, Dictionary<string, hierarchy>> hierarchies = new Dictionary<string, Dictionary<string, hierarchy>>();

        public class table_info
        {
            public Dictionary<string, column_info> columns = new Dictionary<string, column_info>();
            public Dictionary<string, List<string>> links_in = new Dictionary<string, List<string>>(); // table column
            public Dictionary<string, List<string>> links_out = new Dictionary<string, List<string>>();
            public List<column_info> names = new List<column_info>(); // поля образующие name записи
            public string pk = null;
            public string tree = null;
            public string name = null;
            public string name_alias = null;
        }

        public class column_info
        {
            public string name = null;
            public string constraint_type = null;
            public string col_out = null;
            public string tbl_out = null;
            public string data_type = null;
            public bool is_nullable = false;
            public bool is_identity = false;
            public bool is_name = false;
            public int char_length = 0;
            public table_info table_info = null;

            public string name_alias = null;
        }

        //public class hierarchy
        //{
        //    public string name = null;
        //    //public string pk = null;
        //    public Dictionary<string, hierarchy> childs = new Dictionary<string, hierarchy>();
        //    //public Dictionary<string, hierarchy> refs = new Dictionary<string, hierarchy>();
        //}

        //------------------------------------------------------------------------------------------------------------

        public static DataRowCollection get_all_tables() {
            return DBHelper.query(conn, "SELECT * FROM sys.objects WHERE type in (N'U') order by name");
        }
        public static table_info get_table(string table)
        {
            if (!tables.ContainsKey(conn)) tables.Add(conn, new Dictionary<string, table_info>());
            if (tables[conn].ContainsKey(table)) return tables[conn][table];

            table_info ti = new table_info();
            ti.name = table;
            DataRowCollection columns = DBHelper.query(conn, @"
                SELECT c.column_name, ccu.column_name as fk_column, ccu.table_name as fk_table, ccuin.TABLE_NAME as in_table, ccuin.COLUMN_NAME as in_column, CONSTRAINT_TYPE, DATA_TYPE, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMNPROPERTY(object_id(c.TABLE_NAME), c.column_name, 'IsIdentity') as isidentity, iname 
                FROM INFORMATION_SCHEMA.COLUMNS c 
                left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu on kcu.TABLE_SCHEMA = c.TABLE_SCHEMA and kcu.TABLE_CATALOG = c.TABLE_CATALOG and kcu.TABLE_NAME = c.TABLE_NAME and kcu.COLUMN_NAME = c.COLUMN_NAME 
                left join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc on rc.UNIQUE_CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG and rc.UNIQUE_CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA and rc.UNIQUE_CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                left join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu on rc.CONSTRAINT_CATALOG = ccu.CONSTRAINT_CATALOG and rc.CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA and rc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME 
                left join INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME and tc.TABLE_SCHEMA = c.TABLE_SCHEMA and tc.TABLE_CATALOG = c.TABLE_CATALOG and tc.TABLE_NAME = c.TABLE_NAME 
                left join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rcin on rcin.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG and rcin.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA and rcin.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                left join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccuin on rcin.UNIQUE_CONSTRAINT_CATALOG = ccuin.CONSTRAINT_CATALOG and rcin.UNIQUE_CONSTRAINT_SCHEMA = ccuin.CONSTRAINT_SCHEMA and rcin.UNIQUE_CONSTRAINT_NAME = ccuin.CONSTRAINT_NAME 
                
                left join (
	                select syst.name, sysi.name as iname, sysc.name as cname
	                from sys.tables syst
	                inner join sys.indexes sysi on sysi.object_id = syst.object_id and sysi.is_unique = 1 and sysi.type <> 1
	                inner join sys.index_columns sysic on sysi.object_id = sysic.object_id and sysi.index_id = sysic.index_id
	                inner join sys.columns sysc on sysic.object_id = sysc.object_id and sysic.column_id = sysc.column_id
                ) unicidx on unicidx.name = c.TABLE_NAME and unicidx.cname = c.COLUMN_NAME
                
                WHERE c.TABLE_NAME = '" + table + "' order by c.ORDINAL_POSITION;"
            );

            foreach (DataRow row in columns)
            {
                if (!ti.columns.ContainsKey(row["column_name"].ToString()))
                {
                    column_info fk = new column_info() {
                        name = row["column_name"].ToString(),
                        data_type = row["DATA_TYPE"].ToString(),
                        char_length = String.IsNullOrEmpty(row["CHARACTER_MAXIMUM_LENGTH"].ToString()) ? 0 : int.Parse(row["CHARACTER_MAXIMUM_LENGTH"].ToString()),
                        table_info = ti
                    };
                    
                    if (!String.IsNullOrEmpty(row["CONSTRAINT_TYPE"].ToString()))
                    {
                        fk.constraint_type = row["CONSTRAINT_TYPE"].ToString();
                        if(row["CONSTRAINT_TYPE"].ToString() == "PRIMARY KEY") ti.pk = row["column_name"].ToString();
                    }
                    if (!String.IsNullOrEmpty(row["fk_column"].ToString()) && !String.IsNullOrEmpty(row["fk_table"].ToString())) ti.links_in.Add(row["fk_table"].ToString(), new List<string>() { row["fk_column"].ToString() }); //fk.links_in.Add(row["fk_column"].ToString(), new List<string>() { row["fk_table"].ToString() });
                    if (!String.IsNullOrEmpty(row["in_column"].ToString()) && !String.IsNullOrEmpty(row["in_table"].ToString()))
                    {
                        fk.tbl_out = row["in_table"].ToString();
                        fk.col_out = row["in_column"].ToString();
                        if (!ti.links_out.ContainsKey(row["in_table"].ToString())) ti.links_out.Add(row["in_table"].ToString(), new List<string>());
                        ti.links_out[row["in_table"].ToString()].Add(row["column_name"].ToString());
                    }
                    if (row["IS_NULLABLE"].ToString() == "YES") fk.is_nullable = true;
                    if (row["isidentity"].ToString() == "1") fk.is_identity = true;
                    if (!row.IsNull("iname"))
                    {
                        fk.is_name = true;
                        ti.names.Add(fk);
                    }
                    ti.columns.Add(row["column_name"].ToString(), fk);
                }
                else if (!ti.links_in.ContainsKey(row["fk_table"].ToString()))
                {
                    ti.links_in.Add(row["fk_table"].ToString(), new List<string>() { row["fk_column"].ToString() });
                }
                else if (!ti.links_in[row["fk_table"].ToString()].Contains(row["fk_column"].ToString()))
                {
                    ti.links_in[row["fk_table"].ToString()].Add(row["fk_column"].ToString());
                }

                if (row["in_table"].ToString() == table) ti.tree = row["column_name"].ToString(); // пока так
            }

            if (!tables[conn].ContainsKey(table)) tables[conn].Add(table, ti); //хз
            return tables[conn][table];
        }

        //public static hierarchy get_hierarchy(string table)
        //{
        //    if (hierarchies[conn].ContainsKey(table) /*&& hierarchies[conn][table].childs.Count > 0*/) return hierarchies[conn][table];
        //    DataRowCollection drc = DBHelper.query(conn, @"
        //        WITH hierarchy (TABLE_NAME, column_name, table_in, column_in, path)
        //        AS
        //        (
        //            SELECT kcu.TABLE_NAME, kcu.column_name, ccuin.TABLE_NAME, ccuin.COLUMN_NAME, cast(kcu.TABLE_NAME as nvarchar(max))
        //            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
	       //         inner join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rcin on rcin.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG and rcin.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA and rcin.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
	       //         inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccuin on rcin.UNIQUE_CONSTRAINT_CATALOG = ccuin.CONSTRAINT_CATALOG and rcin.UNIQUE_CONSTRAINT_SCHEMA = ccuin.CONSTRAINT_SCHEMA and rcin.UNIQUE_CONSTRAINT_NAME = ccuin.CONSTRAINT_NAME
	       //         WHERE kcu.TABLE_NAME = '" + table + @"'
        //            UNION ALL
        //            SELECT kcu.TABLE_NAME, kcu.column_name, ccuin.TABLE_NAME, ccuin.COLUMN_NAME, path + '/' + kcu.TABLE_NAME
	       //         from INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
	       //         inner join INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rcin on rcin.CONSTRAINT_CATALOG = kcu.CONSTRAINT_CATALOG and rcin.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA and rcin.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
	       //         inner join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccuin on rcin.UNIQUE_CONSTRAINT_CATALOG = ccuin.CONSTRAINT_CATALOG and rcin.UNIQUE_CONSTRAINT_SCHEMA = ccuin.CONSTRAINT_SCHEMA and rcin.UNIQUE_CONSTRAINT_NAME = ccuin.CONSTRAINT_NAME
	       //         inner join hierarchy h on h.table_in = kcu.TABLE_NAME and h.TABLE_NAME <> kcu.TABLE_NAME
        //        )
                
        //        SELECT *
        //        FROM hierarchy
        //        --order by path
        //    ");

        //    foreach (DataRow dr in drc)
        //    {
        //        if (!hierarchies[conn].ContainsKey(dr["TABLE_NAME"].ToString())) hierarchies[conn].Add(dr["TABLE_NAME"].ToString(), new hierarchy() { name = dr["TABLE_NAME"].ToString() });
        //        if (!hierarchies[conn].ContainsKey(dr["table_in"].ToString())) hierarchies[conn].Add(dr["table_in"].ToString(), new hierarchy() { name = dr["table_in"].ToString() });
        //        if (!hierarchies[conn][dr["TABLE_NAME"].ToString()].childs.ContainsKey(dr["column_name"].ToString())) hierarchies[conn][dr["TABLE_NAME"].ToString()].childs.Add(dr["column_name"].ToString(), hierarchies[conn][dr["table_in"].ToString()]);

        //        //hierarchies[conn][dr["table_in"].ToString()].pk = dr["column_in"].ToString(); // предпологаем что все ссылки идут на pk 
        //        //if (!hierarchies[conn][dr["table_in"].ToString()].refs.ContainsKey(dr["column_in"].ToString())) hierarchies[conn][dr["table_in"].ToString()].refs.Add(dr["column_in"].ToString(), hierarchies[conn][dr["TABLE_NAME"].ToString()]);
        //    }

        //    return hierarchies[conn][table];
        //}

        public static Dictionary<string, List<string>> get_query_elements(string table)
        {
            table_info ti = get_table(table);
            Dictionary<string, List<string>> res = new Dictionary<string, List<string>>() { { "select", new List<string>() { "[" + table + "].*" } }, { "left join", new List<string>() } };
            foreach (KeyValuePair<string, List<string>> l in ti.links_out) /*if(table != l.Key)*/ foreach(string c in l.Value){
                table_info ti_ref = get_table(l.Key);
                List<string> names_ref = new List<string>();
                List<column_info> cols = ti_ref.names.Count > 0 ? ti_ref.names : ti_ref.columns.Values.ToList();
                foreach (column_info ci in cols)
                {
                    if (ci.tbl_out == null)
                    {
                        if(ci.data_type.Contains("char") || ci.data_type == "text") names_ref.Add("[" + l.Key + "_" + c + "].[" + ci.name + "]");
                        else if(!ci.is_identity ||(ci.is_identity && ci.is_name)) names_ref.Add("cast([" + l.Key + "_" + c + "].[" + ci.name + "] as nvarchar(max))");
                    }
                }
                res["select"].Add("isnull(" + string.Join(", '') + ' ' + isnull(", names_ref.ToArray()) + ", '') as [" + c + "_ref]");
                res["left join"].Add("[" + l.Key + "] as [" + l.Key + "_" + c + "]" + " on [" + l.Key + "_" + c + "].[" + ti_ref.pk + "] = [" + table + "].[" + c + "]");
            }
            return res;
        }

        //public static string get_col_names(string table, int idx = 0)
        //{
        //    string alias = idx > 0 ? "kolap" + idx.ToString() : table;
        //    List<string> sel = new List<string>();
        //    List<string> sel_all = new List<string>();
        //    if (!tables[conn].ContainsKey(table)) get_table(table);
        //    foreach (KeyValuePair<string, column_info> col in tables[conn][table].columns)
        //        if (col.Value.constraint_type != "PRIMARY KEY")
        //        {
        //            string col_str = "isnull(" + (col.Value.data_type.IndexOf("char") >= 0 ? "[" + alias + "].[" + col.Key + "]" : "cast([" + alias + "].[" + col.Key + "] as nvarchar(max))") + ", '')";
        //            sel_all.Add(col_str); // что то не пошло ключи заджоинить (( string.Join(" + ' ' + ", db.data_source[Node.Parent.Parent.Text].structure[Node.Parent.Text][Node.Text].Keys)
        //            if (col.Value.is_name) sel.Add(col_str);
        //        }

        //    return sel.Count > 0 ? string.Join(" + ' ' + ", sel.ToArray()) : string.Join(" + ' ' + ", sel_all.ToArray());
        //}

        //public static string get_col_full_names(string table)
        //{
        //    hierarchy h = get_hierarchy(table);
        //    if (h.childs.Count == 0) return get_col_names(table);
        //    List<string> res = new List<string>();
        //    foreach (KeyValuePair<string, hierarchy> hh in h.childs) res.Add(get_col_full_names(hh.Value.name));
        //    return string.Join(" + ' ' + ", res);
        //}

        //----------------------------------------------------------------------------------

        public static DataRowCollection get_data_all(string table, Dictionary<string, List<string>> filter = null)
        {
            table_info ti = get_table(table);
            List<string> names = new List<string>();
            List<column_info> cols = ti.names.Count > 0 ? ti.names : ti.columns.Values.ToList();
            foreach (column_info ci in cols)
            {
                if (ci.tbl_out == null)
                {
                    if (ci.data_type.Contains("char") || ci.data_type == "text") names.Add("[" + ci.name + "]");
                    //else if (!ci.is_identity || (ci.is_identity && ci.is_name)) names.Add("cast([" + ci.name + "] as nvarchar(max))");
                }
            }
            string sql = ti.tree == null ? "" : @"
                WITH tree (id, name, parent, level, path_name)
                AS
                (
                    SELECT [" + ti.pk + @"], cast(" + string.Join(" + ' ' + ", names) + @" as nvarchar(max)), [" + ti.tree + @"], 0, cast([" + ti.pk + @"] as nvarchar(max))
                    FROM [" + table + @"]
                    WHERE [" + ti.tree + @"] is null
                    UNION ALL
                    SELECT childs.[ИД], cast(REPLICATE ( N'-' , 2 * (tree.level + 1) ) + childs." + string.Join(" + ' ' + childs.", names) + " as nvarchar(max)), childs." + ti.tree + @", tree.level + 1, tree.path_name + '/' + cast([" + ti.pk + @"] as nvarchar(max))
                    FROM [" + table + @"] childs
                    INNER JOIN tree ON childs.[" + ti.tree + @"] = tree.id
                )
            ";

            Dictionary<string, List<string>> query_elements = get_query_elements(table);
            List<string> where = new List<string>();
            if (filter != null)
                foreach (KeyValuePair<string, List<string>> exp in filter) if (exp.Value.Count > 0)
                    {
                        bool nullval = false;
                        if (exp.Value.Contains(null))
                        {
                            nullval = true;
                            exp.Value.RemoveAll(string.IsNullOrEmpty);
                        }
                        string field = exp.Key.Length > 4 && exp.Key.Substring(exp.Key.Length - 4) == ":not"? exp.Key.Substring(exp.Key.Length - 4): exp.Key;
                        where.Add(
                            "("
                            + (exp.Value.Count > 0 ? "[" + table + "].[" + field + "] " + (field != exp.Key? "not": "") + " in (" + String.Join(", ", exp.Value.ToArray()) + ")" : "")
                            + (nullval && exp.Value.Count > 0 ? " or " : "")
                            + (nullval ? "[" + table + "].[" + field + "] is " + (field != exp.Key ? "not" : "") + " null" : "")
                            + ")"
                        ); // ?? exp.Value.Count > 0 ?? лучше спроектировать, проследить от куда пусто
                    }

            sql += " SELECT " + string.Join(", ", query_elements["select"].ToArray()) + (ti.tree == null? "": ", level")
                + " FROM [" + table + "] " 
                + (ti.tree == null ? "": "inner join tree on tree.id = [" + table + "].[" + ti.pk + "] ")
                + (query_elements["left join"].Count() > 0? " left join " + string.Join(" left join ", query_elements["left join"].ToArray()): "")
                + (where.Count > 0 ? " where " + string.Join(" and ", where.ToArray()) : "")
                + (ti.tree == null ? "" : " order by path_name");

            
            return DBHelper.query(conn, sql);
        }

        //public static DataRowCollection get_data_tree(string table, Dictionary<string, List<string>> filter = null, int space = 2)
        //{
        //    table_info ti = get_table(table);
        //    string name = string.Join(" + ' ' + ", get_col_full_names(table));
        //    string sql = @"
        //        WITH tree (id, name, parent, level, path_name)
        //        AS
        //        (
        //            SELECT [" + ti.pk + @"], cast(" + name + @" as nvarchar(max)), [" + ti.tree + @"], 0, cast([" + ti.pk + @"] as nvarchar(max))
        //            FROM [" + table + @"]
        //            WHERE [" + ti.tree + @"] is null
        //            UNION ALL
        //            SELECT childs.[ИД], cast(REPLICATE ( N'-' , " + space.ToString() + @" * (tree.level + 1) ) + childs.[Наименование] as nvarchar(max)), childs.[" + ti.tree + @"], tree.level + 1, tree.path_name + '/' + cast([" + ti.pk + @"] as nvarchar(max))
        //            FROM [" + table + @"] childs
        //            INNER JOIN tree ON childs.[" + ti.tree + @"] = tree.id
        //        )
                
        //        SELECT t.*
        //        FROM tree t
        //        order by path_name
        //    ";
        //    // + join
        //    return DBHelper.query(conn, sql);
        //}

        //----------------------------------------------------------------
        public static string insert(string table, NameValueCollection post)
        {
            string values = ("'" + String.Join("', '", post.Cast<string>().Select(i => post[i])) + "'").Replace("'null'", "null").Replace("''", "null");
            DataRowCollection drc = DBHelper.query(conn, "insert into [" + table + "] ([" + String.Join("], [", post.AllKeys) + "]) values (" + values + "); SELECT IDENT_CURRENT('" + table + "') as id;"); // SCOPE_IDENTITY() as id // на случай с тригерами
            return drc.Cast<DataRow>().ToArray()[0]["id"].ToString();
        }

        public static void update(string table, string id_col, string id_val, NameValueCollection post)
        {
            DBHelper.query(conn, "update [" + table + "] set " + String.Join(", ", post.Cast<string>().Select(i => "[" + i + "] = '" + post[i] + "'")).Replace("''", "null") + " where " + id_col + " = " + id_val); // post.ToString().Replace("&", ", ")
        }

        public static void delete(string table, string id_name, string id_val) {
            DBHelper.query(conn, "delete from [" + table + "] where " + id_name + " = " + id_val);
        }

        public static Dictionary<string, DataRowCollection> dirs(string table) {
            table_info ti = get_table(table);
            Dictionary<string, DataRowCollection> dirs = new Dictionary<string, DataRowCollection>();
            foreach (KeyValuePair<string, column_info> ci in ti.columns)
                if (ci.Value.tbl_out != null && !dirs.ContainsKey(ci.Value.tbl_out)) dirs.Add(ci.Key, get_data_all(ci.Value.tbl_out)); //dirs.Add(ci.Key, dir(ci.Value.tbl_out, ci.Value.col_out));

            return dirs;
        }

        public static string get_name_by_row(string table, DataRow row) {
            List<string> res = new List<string>();
            table_info ti = get_table(table);
            List<column_info> cols = ti.names.Count > 0 ? ti.names : ti.columns.Values.ToList();
            foreach (column_info ci in cols) res.Add(row.Table.Columns.Contains(ci.name + "_ref")? row[ci.name + "_ref"].ToString() : row[ci.name].ToString());
            return string.Join(" ", res.ToArray());
        }

        //--------------------------------------------------------------------------------------

        public static void approve_columns(string table, string[] cols) {
            kolap.table_info ti = kolap.get_table(table);
            foreach (string del in ti.columns.Keys.ToList().Except(cols)) ti.columns.Remove(del);
        }
    }
}