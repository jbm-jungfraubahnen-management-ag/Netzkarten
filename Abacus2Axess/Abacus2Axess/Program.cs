using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Threading;
using MySql;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Collections;
using System.Diagnostics;

namespace Abacus2Axess
{




    class Program
    {




        static void Main(string[] args)
        {
            try
            {
                WriteEventLog("Start Abacus Sync: " + DateTime.Now.ToLongTimeString());
                Console.Write(args[0]);
            }
            catch (Exception cmdl)
            {
                Console.WriteLine("invalid Argument... Please run this command like Abacus2Axess.exe c:\\path\\file.xml");
                return;
            }

            string SQLTabelle = "";

            Hashtable myParameter = getSettings(args[0]);
            String DirectoryName = @"\\dir-aba17\abacusv2017prod\Exp_SS\Axess\Netzkarten";

            DirectoryName = myParameter["XMLPath"].ToString();
            string[] files = Directory.GetFiles(myParameter["IMGPath"].ToString());

            if (myParameter["SQLTabelle"].ToString().IndexOf("Test") >= 0)
            {
                SQLTabelle = "T_NETZKARTE_TEST";
                MySQLInsertWithErrorControl("TRUNCATE TABLE T_NETZKARTE_IMAGE_TEST;", myParameter["ServerIP"].ToString(), myParameter["PortNr"].ToString(), myParameter["UserID"].ToString(), myParameter["Password"].ToString(), myParameter["Database"].ToString());
            }
            else
            {
                SQLTabelle = "T_NETZKARTE";
                MySQLInsertWithErrorControl("TRUNCATE TABLE T_NETZKARTE_IMAGE;", myParameter["ServerIP"].ToString(), myParameter["PortNr"].ToString(), myParameter["UserID"].ToString(), myParameter["Password"].ToString(), myParameter["Database"].ToString());
            }



            List<string> FileList = new List<string>();
            int ImagesInserted = 0;
            foreach (string f in files)
            {
                FileList.Add(f);
                InsertImage(FileList, f, myParameter);
                FileList.Clear();
                ImagesInserted++;
            }
            WriteEventLog("Abacus Sync Images inserted: " + ImagesInserted.ToString() + " at " + DateTime.Now.ToLongTimeString());
            ImagesInserted = 0;

            int a = MySQLInsertWithErrorControl("TRUNCATE TABLE " + SQLTabelle + ";", myParameter["ServerIP"].ToString(), myParameter["PortNr"].ToString(), myParameter["UserID"].ToString(), myParameter["Password"].ToString(), myParameter["Database"].ToString());
            XmlDocument xDoc = new XmlDocument();
            //xDoc.Load(@"F:\home\rbi\source\JB\abacus\Netzkarten\Export_Netzkarten_Axess.xml");

            //xDoc.Load(DirectoryName.Replace("\\", "\\\\") + "\\JB_Export_Netzkarten_Axess.xml");
            xDoc.Load(DirectoryName);
            int total = 0;
            XmlNodeList elemList = xDoc.GetElementsByTagName("Activity");
            for (int i = 0; i < elemList.Count; i++)
            {
                foreach (XmlNode childNode in elemList)

                {
                    try
                    {
                        string Seperator = "','";
                        string AddressNumber = childNode.SelectSingleNode("AddressNumber").InnerText.Replace("'", "''");
                        string ContactNumber = childNode.SelectSingleNode("AKT.AKP_NR").InnerText.Replace("'", "''");
                        string Name_ = childNode.SelectSingleNode("EXPR_Name").InnerText.Replace("'", "''");
                        string Vorname_ = childNode.SelectSingleNode("EXPR_Vorname").InnerText.Replace("'", "''");
                        string Strasse = childNode.SelectSingleNode("ADR.ZEILE2_Mitarbeiter").InnerText.Replace("'", "''");
                        string PLZ = childNode.SelectSingleNode("ADR.PLZ_Mitarbeiter").InnerText.Replace("'", "''");
                        string ORT = childNode.SelectSingleNode("ADR.ORT_Mitarbeiter").InnerText.Replace("'", "''");
                        string Geburtsdatum_ = childNode.SelectSingleNode("EXPR_Geburtsdatum").InnerText.Replace("'", "''");
                        string ActivityType = childNode.SelectSingleNode("AKT.AKA_ID").InnerText.Replace("'", "''");
                        string _USERFIELD7 = childNode.SelectSingleNode("AKT._USERFIELD7").InnerText.Replace("'", "''");
                        string EXPR_Karte_gueltig_bis = childNode.SelectSingleNode("EXPR_Karte_gueltig_bis").InnerText.Replace("'", "''");
                        string EXPR_1_Klasse = childNode.SelectSingleNode("EXPR_1_Klasse").InnerText.Replace("'", "''");
                        string Bild_ = childNode.SelectSingleNode("EXPR_Bild_").InnerText.Replace("'", "''");
                        string Karten_Nr_ = childNode.SelectSingleNode("EXPR_Karten_Nr_").InnerText.Replace("'", "''");
                        string sql = "insert into \"" + SQLTabelle + "\" (\"AddressNumber\", \"AKT.AKP_NR\", \"EXPR_Name\", \"EXPR_Vorname\", \"ADR.ZEILE2_Mitarbeiter\", \"ADR.PLZ_Mitarbeiter\", \"ADR.ORT_Mitarbeiter\", \"EXPR_Geburtsdatum\", \"AKT.AKA_ID\",";
                        sql += "\"AKT._USERFIELD7\",  \"EXPR_Karte_gueltig_bis\", \"EXPR_1_Klasse\",  \"EXPR_Bild_\", \"EXPR_Karten_Nr_\") values ('";
                        sql += AddressNumber + Seperator;
                        sql += ContactNumber + Seperator;
                        sql += Name_ + Seperator;
                        sql += Vorname_ + Seperator;
                        sql += Strasse + Seperator;
                        sql += PLZ + Seperator;
                        sql += ORT + Seperator;
                        sql += Geburtsdatum_ + Seperator;

                        sql += ActivityType + Seperator;
                        sql += _USERFIELD7 + Seperator;
                        sql += EXPR_Karte_gueltig_bis + Seperator;
                        sql += EXPR_1_Klasse + Seperator;
                        sql += Bild_ + Seperator;
                        sql += Karten_Nr_ + "');";
                        if (total <= elemList.Count)
                        {

                            MySQLInsertWithErrorControl(sql, myParameter["ServerIP"].ToString(), myParameter["PortNr"].ToString(), myParameter["UserID"].ToString(), myParameter["Password"].ToString(), myParameter["Database"].ToString());
                            ImagesInserted++;

                        }
                        else
                        {
                            break;
                        }
                        total++;

                    }
                    catch (Exception df)
                    {
                        df.ToString();
                    }


                }
            }
            WriteEventLog("Abacus Sync PersonRecords inserted: " + ImagesInserted.ToString() + " at " + DateTime.Now.ToLongTimeString());
            WriteEventLog("Abacus Sync successfully terminated: " + " at " + DateTime.Now.ToLongTimeString());
        }

        static void InsertImage(List<string> AllFiles, string t_netzkarte_bild, Hashtable myParameter)
        {


            if (t_netzkarte_bild.IndexOf("\\") >= 0)
            {
                t_netzkarte_bild = t_netzkarte_bild.Substring(t_netzkarte_bild.LastIndexOf("\\") + 1);
                if (t_netzkarte_bild.IndexOf("ADR") >= 0)
                {
                    t_netzkarte_bild = t_netzkarte_bild.Substring(t_netzkarte_bild.IndexOf("ADR") + "ADR".Length).ToLower();
                    if (t_netzkarte_bild.IndexOf(".bmp") >= 0)
                    {
                        t_netzkarte_bild = t_netzkarte_bild.Replace(".bmp", "");
                    }
                }
            }
            foreach (string FileName in AllFiles)
            {

                byte[] Buffer = null;
                try
                {
                    FileStream fstPDF = new FileStream(FileName, FileMode.Open);
                    BinaryReader readPDF = new BinaryReader(fstPDF);

                    Buffer = readPDF.ReadBytes((int)fstPDF.Length);
                    //var creation = File.GetCreationTimeUtc(FileName);
                    FileInfo fi = new FileInfo(FileName);
                    var created = fi.CreationTime;
                    var lastmodified = fi.LastWriteTime;
                    string tmpFileName = FileName.Substring(FileName.LastIndexOf("\\") + 1);
                    if (myParameter["SQLTabelle"].ToString().IndexOf("Test") >= 0)
                    {
                        BinToMySQL("T_NETZKARTE_IMAGE_TEST", tmpFileName, Buffer, created, t_netzkarte_bild, myParameter["ServerIP"].ToString(), myParameter["PortNr"].ToString(), myParameter["UserID"].ToString(), myParameter["Password"].ToString(), myParameter["Database"].ToString());
                    }
                    else
                    {
                        BinToMySQL("T_NETZKARTE_IMAGE", tmpFileName, Buffer, created, t_netzkarte_bild, myParameter["ServerIP"].ToString(), myParameter["PortNr"].ToString(), myParameter["UserID"].ToString(), myParameter["Password"].ToString(), myParameter["Database"].ToString());
                    }
                    //public void BinToPostgres(string Connection, string FileName, Byte[] BinData, string t_netzkarte_bild)
                }
                catch (Exception bine)
                {
                    Buffer = null;
                }
            }
        }



        static Hashtable getSettings(string path)
        {
            Hashtable _ret = new Hashtable();

            int a = 2;
            if (File.Exists(path))
            {
                StreamReader reader = new StreamReader
                (
                    new FileStream(
                        path,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.Read)
                );
                XmlDocument doc = new XmlDocument();
                string xmlIn = reader.ReadToEnd();
                reader.Close();
                doc.LoadXml(xmlIn);
                foreach (XmlNode child in doc.ChildNodes)
                    if (child.Name.Equals("Settings"))
                        foreach (XmlNode node in child.ChildNodes)
                            if (node.Name.Equals("add"))
                                _ret.Add
                                (
                                    node.Attributes["key"].Value,
                                    node.Attributes["value"].Value
                                );
            }
            return (_ret);
        }


        public string QuoteMysqlSchemaName(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("name must have a value");

            const int maxLength = 64;
            if (name.Length > maxLength)
                throw new ArgumentException(string.Format("name is longer than {0} characters", maxLength));

            var prohibited = new[] { '\0', '.', '/', '\\' };
            foreach (var c in name)
            {
                if (prohibited.Contains(c))
                    throw new ArgumentException("name may not contain: '.', '/', or '\\'");

                if (char.IsHighSurrogate(c) || char.IsLowSurrogate(c))
                    throw new ArgumentException("name may not contain unicode supplementary characters");
            }

            if (name.EndsWith(" "))
                throw new ArgumentException("name may not end with a space");

            return "`" + name.Replace("`", "``") + "`";
        }
        static int MySQLInsertWithErrorControl(string sqlStatement, string sServer, string sPortNr, string sUserID, string sPassword, string sDB)
        {
            sqlStatement = sqlStatement.Replace("\"", "`");

            {

                MySqlConnectionStringBuilder conn_string = new MySqlConnectionStringBuilder();


                conn_string.Server = sServer;
                conn_string.UserID = sUserID;
                conn_string.Password = sPassword;
                conn_string.Database = sDB;
                conn_string.Port = Convert.ToUInt16(sPortNr);

                try
                {
                    using (MySqlConnection dbConnection = new MySqlConnection(conn_string.ToString()))
                    {
                        dbConnection.Open();
                        MySqlCommand dbCommand = new MySqlCommand(sqlStatement, dbConnection);
                        int result = dbCommand.ExecuteNonQuery();
                        dbConnection.Close();
                    }
                    return (0);
                }
                catch (Exception posteq)
                {
                    string myerror = posteq.ToString();
                    if (myerror.IndexOf("duplicate") >= 0)
                    {
                    }
                    else
                    {
                        ////MessageBox.Show(posteq.ToString());
                        //    WriteEventLog("SQL-ERROR", sqlStatement.Replace("'", "*"), "0", posteq.ToString().Replace("'", "*"), "0", "0");
                    }
                    return (-1);
                }
            }


        }

        static void WriteEventLog(string mymessage)
        {
            using (EventLog eventLog = new EventLog("Application"))
            {
                eventLog.Source = "Application";
                eventLog.WriteEntry(mymessage, EventLogEntryType.Information, 101, 1);
            }
        }




        static void BinToMySQL(string SQLTab, string FileName, Byte[] BinData, DateTime FileDate, string t_netzkarte_bild, string sServer, string sPortNr, string sUserID, string sPassword, string sDB)
        {
            MySqlConnectionStringBuilder conn_string = new MySqlConnectionStringBuilder();

            conn_string.Server = sServer;
            conn_string.UserID = sUserID;
            conn_string.Password = sPassword;
            conn_string.Database = sDB;
            conn_string.Port = Convert.ToUInt16(sPortNr);



            using (MySqlConnection pgConnection = new MySqlConnection(conn_string.ToString()))
            {
                try
                {

                    using (MySqlCommand pgCommand = new MySqlCommand("insert into " + SQLTab + " (datetime, filename, bindata, t_netzkarte_bild) SELECT @DateTime, @FileName, @BinData, @t_netzkarte_bild ", pgConnection))
                    {
                        pgCommand.Parameters.AddWithValue("@DateTime", FileDate);
                        pgCommand.Parameters.AddWithValue("@FileName", FileName);
                        pgCommand.Parameters.AddWithValue("@BinData", BinData);
                        pgCommand.Parameters.AddWithValue("@t_netzkarte_bild", t_netzkarte_bild);


                        try
                        {
                            pgConnection.Open();
                            pgCommand.ExecuteNonQuery();
                        }
                        catch (Exception df)
                        {
                            //  throw;
                        }
                    }


                }
                catch
                {
                    throw;
                }

            }
        }


    }
}






