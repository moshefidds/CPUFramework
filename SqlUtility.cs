using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace CPUFramework
{
    public class SqlUtility
    {
        public static string ConnectionString = "";
        // GetDataTable - take a sql statement and retaurn a data table
        public static DataTable GetDataTable(string sqlstatement)
        {
            Debug.Print(sqlstatement);
            DataTable dt = new();
            SqlConnection conn = new();
            conn.ConnectionString = ConnectionString;
            conn.Open();

            var cmd = new SqlCommand();
            cmd.Connection = conn;
            cmd.CommandText = sqlstatement;
            var dr = cmd.ExecuteReader();
            dt.Load(dr);
            return dt;
        }
    }
}
